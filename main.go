package main

import (
	"context"
	"encoding/json"
	"fmt"
	"judge/worker"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

type JobData struct {
	Problem_data    worker.ProblemData    `json:"problem_data"`
	Submission_data worker.SubmissionData `json:"submission_data"`
}

func main() {
	connection, err := amqp091.Dial("amqp://127.0.0.1:5672")
	if err != nil {
		log.Fatalf("Could not connect to the RabbitMQ server")
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Could not establish a channel on the RabbitMQ server")
	}
	defer channel.Close()

	SUBMISSION_QUEUE_NAME := "submission_queue"
	_, err = channel.QueueDeclare(SUBMISSION_QUEUE_NAME, true, false, false, false, nil)

	if err != nil {
		log.Println("Failed to declare the queue")
	}

	fmt.Printf("Waiting for messages in the \"%s.\" To exit press CTRL+C\n", SUBMISSION_QUEUE_NAME)
	msgs, err := channel.Consume(SUBMISSION_QUEUE_NAME, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Failed to register a consumer on the \"%s\" queue\nError: %s", SUBMISSION_QUEUE_NAME, err)
	}

	for msg := range msgs {
		fmt.Printf("Recieved submission %s for judging\n", msg.CorrelationId)

		var job JobData
		err = json.Unmarshal(msg.Body, &job)
		if err != nil {
			log.Printf("Failed to unmarshal json %s into a struct object of type %T", string(msg.Body), job)
		}

		result := worker.ProcessSubmission(job.Submission_data, job.Problem_data)

		responseBody, err := json.Marshal(result)
		if err != nil {
			log.Fatalf("Failed to convert %+v into JSON using json.Marhsal()", result)
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
	}

}
