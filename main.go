package main

import (
	"context"
	"encoding/json"
	"fmt"
	"judge/worker"
	"log"

	"github.com/caarlos0/env/v9"
	"github.com/rabbitmq/amqp091-go"
)

type config struct {
	RABBIT_MQ_CONNECTION_STRING string `env:"RABBIT_MQ_CONNECTION_STRING" envDefault:"amqp://127.0.0.1:5672"`
}

type JobData struct {
	Problem_data    worker.ProblemData    `json:"problem_data"`
	Submission_data worker.SubmissionData `json:"submission_data"`
}

func main() {

	//Initialize env variables
	cfg := config{}
	err := env.Parse(&cfg)
	if err != nil {
		log.Panicf("Failed to parse environment variables.")
	}

	connection, err := amqp091.Dial(cfg.RABBIT_MQ_CONNECTION_STRING)
	if err != nil {
		log.Panicf("Could not connect to the RabbitMQ server")
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Panicf("Could not establish a channel on the RabbitMQ server")
	}
	defer channel.Close()

	SUBMISSION_QUEUE_NAME := "submission_queue"
	_, err = channel.QueueDeclare(SUBMISSION_QUEUE_NAME, true, false, false, false, nil)

	if err != nil {
		log.Panicf("Failed to declare the queue")
	}

	fmt.Printf("Waiting for messages in the \"%s.\" To exit press CTRL+C\n", SUBMISSION_QUEUE_NAME)
	msgs, err := channel.Consume(SUBMISSION_QUEUE_NAME, "", false, false, false, false, nil)
	if err != nil {
		log.Panicf("Failed to register a consumer on the \"%s\" queue\nError: %s", SUBMISSION_QUEUE_NAME, err)
	}

	for msg := range msgs {
		fmt.Printf("Received submission %s for judging\n", msg.CorrelationId)

		var job JobData
		err = json.Unmarshal(msg.Body, &job)
		if err != nil {
			log.Printf("Failed to unmarshal JSON %s into a struct object of type %T", string(msg.Body), job)
			// TODO: Handle the error properly
			continue
		}

		go func(msg amqp091.Delivery, job JobData) {
			result := worker.ProcessSubmission(&job.Submission_data, &job.Problem_data)

			responseBody, err := json.Marshal(result)
			if err != nil {
				log.Fatalf("Failed to convert %+v into JSON using json.Marshal()", result)
			}

			err = channel.PublishWithContext(context.Background(), "", msg.ReplyTo, false, false, amqp091.Publishing{
				ContentType:   "application/json",
				CorrelationId: msg.CorrelationId,
				Body:          responseBody,
			})
			if err != nil {
				log.Fatalf("Failed to publish response: %s", err)
			}

			// Acknowledge the message
			err = msg.Ack(false)
			if err != nil {
				log.Printf("Failed to acknowledge message: %s", err)
			}
		}(msg, job)
	}
}
