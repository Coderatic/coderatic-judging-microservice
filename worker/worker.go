package worker

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/google/uuid"
)

func ProcessSubmission(sub SubmissionData, problem ProblemData) (result JobResult) {
	result = JobResult{
		Verdict:             "IE",
		Cpu_time:            0,
		Memory:              0,
		Sample_test_results: []TestResult{},
		Hidden_test_results: []TestResult{},
	}

	src_name := uuid.New().String()
	exec_path := filepath.Join(CODE_DIR, src_name)
	src_path := exec_path + "." + problem.Language.Extension
	escaped_src := escapeSrc(&sub.Source_code)

	if err := os.MkdirAll(filepath.Dir(src_path), 0755); err != nil {
		log.Printf("Failed to create directory: %s", err)
		return
	}

	if err := os.WriteFile(src_path, []byte(escaped_src), 0644); err != nil {
		log.Printf("Failed to create a file with escaped source code at %s", src_path)
		return
	}
	defer deleteFile(src_path)

	if problem.Language.Is_compiled {
		cmd := exec.Command("./compile.sh", problem.Language.Name, src_name)
		cmd.Dir = SCRIPTS_DIR

		if err := cmd.Start(); err != nil {
			log.Printf("Failed to start compilation script:\n%s", err)
			result.Verdict = "IE"
			return
		}

		if err := cmd.Wait(); err != nil {
			result.Verdict = "CE"
			return
		}
		defer deleteFile(exec_path)
	}

	const TEST_CASES_DIR = "worker/cache/test_cases"
	if err := os.MkdirAll(filepath.Dir(TEST_CASES_DIR), 0755); err != nil {
		log.Printf("Failed to create the test_cases directory: %s", err)
		return
	}

	input_dir := filepath.Join(TEST_CASES_DIR, problem.Slug, "input")
	output_dir := filepath.Join(TEST_CASES_DIR, problem.Slug, "output")

	if err := os.MkdirAll(input_dir, 0755); err != nil {
		log.Printf("Failed to create the test cases input directory: %s", err)
		return
	}

	if err := os.MkdirAll(output_dir, 0755); err != nil {
		log.Printf("Failed to create the test cases output directory: %s", err)
		return
	}

	defer aggregateResults(&result)
	// Judging
	for i, sample_test := range problem.Sample_tests {
		input_file_name := "sample-" + strconv.Itoa(i+1) + ".in"
		output_file_name := "sample-" + strconv.Itoa(i+1) + ".out"

		// Paths relative to the root directory
		input_file_path := filepath.Join(input_dir, input_file_name)
		output_file_path := filepath.Join(output_dir, output_file_name)

		// Writing samples to a file
		if err := os.WriteFile(input_file_path, []byte(sample_test.Test_input), 0644); err != nil {
			log.Printf("Failed to write sample test input into a file %s", src_path)
			return
		}
		if err := os.WriteFile(output_file_path, []byte(sample_test.Test_output), 0644); err != nil {
			log.Printf("Failed to write sample test output into a file %s", src_path)
			return
		}

		// Paths relative to the scripts directory
		rel_input_path, _ := filepath.Rel(SCRIPTS_DIR, filepath.Join(input_dir, input_file_name))
		rel_output_path, _ := filepath.Rel(SCRIPTS_DIR, filepath.Join(output_dir, output_file_name))
		rel_exec_path, _ := filepath.Rel(SCRIPTS_DIR, exec_path)

		cmd := exec.Command(
			"python3", "judge.py", problem.Slug, rel_exec_path, problem.Language.Name,
			rel_input_path, rel_output_path,
			strconv.Itoa(problem.Mem_lim), strconv.Itoa(problem.Time_lim),
			"./THE_JUDGE.out")

		cmd.Dir = SCRIPTS_DIR

		var stdout bytes.Buffer
		cmd.Stdout = &stdout

		if err := cmd.Start(); err != nil {
			log.Printf("Failed to start judging script:\n%s", err)
			result.Verdict = "IE"
			return
		}

		if err := cmd.Wait(); err != nil {
			log.Printf("Failed to wait for the judging script to finish:\n%s", err)
			result.Verdict = "IE"
			return
		}

		// Unmarshalling judge output
		var judge_output JudgeOutput
		if err := json.Unmarshal(stdout.Bytes(), &judge_output); err != nil {
			log.Printf("Failed to unmarshal judge output:\n%s", err)
			result.Verdict = "IE"
			return
		}

		result.Sample_test_results = append(result.Sample_test_results, TestResult{
			Verdict:  judge_output.Checker_output.Verdict,
			Cpu_time: judge_output.Cpu_time,
			Memory:   judge_output.Memory,
		})

		if judge_output.Checker_output.Verdict != "AC" {
			return
		}
	}

	for _, hidden_test := range problem.Hidden_tests {

		// Paths relative to the scripts directory
		rel_input_path, _ := filepath.Rel(SCRIPTS_DIR, filepath.Join(input_dir, hidden_test.Input_file_path))
		rel_output_path, _ := filepath.Rel(SCRIPTS_DIR, filepath.Join(output_dir, hidden_test.Output_file_path))
		rel_exec_path, _ := filepath.Rel(SCRIPTS_DIR, exec_path)

		cmd := exec.Command(
			"python3", "judge.py", problem.Slug, rel_exec_path, problem.Language.Name,
			rel_input_path, rel_output_path,
			strconv.Itoa(hidden_test.Mem_lim), strconv.Itoa(hidden_test.Time_lim),
			"./THE_JUDGE.out")

		cmd.Dir = SCRIPTS_DIR

		var stdout bytes.Buffer
		cmd.Stdout = &stdout

		if err := cmd.Start(); err != nil {
			log.Printf("Failed to start judging script:\n%s", err)
			result.Verdict = "IE"
			return
		}

		if err := cmd.Wait(); err != nil {
			log.Printf("Failed to wait for the judging script to finish:\n%s", err)
			result.Verdict = "IE"
			return
		}

		// Unmarshalling judge output
		var judge_output JudgeOutput
		if err := json.Unmarshal(stdout.Bytes(), &judge_output); err != nil {
			log.Printf("Failed to unmarshal judge output:\n%s", err)
			result.Verdict = "IE"
			return
		}

		result.Hidden_test_results = append(result.Hidden_test_results, TestResult{
			Verdict:  judge_output.Checker_output.Verdict,
			Cpu_time: judge_output.Cpu_time,
			Memory:   judge_output.Memory,
		})

		if judge_output.Checker_output.Verdict != "AC" {
			return
		}
	}

	return result
}

func aggregateResults(result *JobResult) JobResult {
	final_verdict := "IE"
	max_cpu_time := 0
	max_memory := 0
	if len(result.Sample_test_results) > 0 {
		final_verdict = result.Sample_test_results[len(result.Sample_test_results)-1].Verdict
	}
	if final_verdict == "AC" && len(result.Hidden_test_results) > 0 {
		final_verdict = result.Hidden_test_results[len(result.Hidden_test_results)-1].Verdict
	}
	result.Verdict = final_verdict

	for _, sample_test := range result.Sample_test_results {
		max_cpu_time = max(max_cpu_time, sample_test.Cpu_time)
		max_memory = max(max_memory, sample_test.Memory)
	}
	for _, hidden_test := range result.Hidden_test_results {
		max_cpu_time = max(max_cpu_time, hidden_test.Cpu_time)
		max_memory = max(max_memory, hidden_test.Memory)
	}
	result.Cpu_time = max_cpu_time
	result.Memory = max_memory
	return *result
}

func escapeSrc(sourceCode *string) string {
	re := regexp.MustCompile(`[\*]`)
	escapedCode := re.ReplaceAllStringFunc(*sourceCode, func(match string) string {
		return match
	})
	return escapedCode
}

func deleteFile(filepath string) {
	err := os.Remove(filepath)
	if err != nil {
		log.Printf("Failed to remove file %s", filepath)
	}

}

const CACHE_DIR = "worker/cache"
const CODE_DIR = "worker/cache/code"
const SCRIPTS_DIR = "worker/scripts"

type SampleTest struct {
	Test_input  string `json:"test_input"`
	Test_output string `json:"test_output"`
}
type HiddenTest struct {
	Input_file_path  string `json:"input_file_path"`
	Output_file_path string `json:"output_file_path"`
	Time_lim         int    `json:"time_lim"`
	Mem_lim          int    `json:"mem_lim"`
}
type Language struct {
	Name        string `json:"name"`
	Extension   string `json:"extension"`
	Is_compiled bool   `json:"is_compiled"`
}
type ProblemData struct {
	Slug         string       `json:"slug"`
	Time_lim     int          `json:"time_lim"`
	Mem_lim      int          `json:"mem_lim"`
	Language     Language     `json:"lang"`
	Sample_tests []SampleTest `json:"sample_tests"`
	Hidden_tests []HiddenTest `json:"hidden_tests"`
}

type SubmissionData struct {
	Id              string `json:"id"`
	User_id         string `json:"user_id"`
	Source_code     string `json:"source_code"`
	Problem_id      string `json:"problem_id"`
	Submission_time string `json:"submission_time"`
}

type TestResult struct {
	Verdict  string `json:"verdict"`
	Cpu_time int    `json:"cpu_time"`
	Memory   int    `json:"memory"`
}

type JobResult struct {
	Verdict             string       `json:"verdict"`
	Cpu_time            int          `json:"cpu_time"`
	Memory              int          `json:"memory"`
	Sample_test_results []TestResult `json:"sample_test_results"`
	Hidden_test_results []TestResult `json:"hidden_test_results"`
}

type JudgeOutput struct {
	Program_exit_code int `json:"program_exit_code"`
	Checker_exit_code int `json:"checker_exit_code"`
	Cpu_time          int `json:"cpu_time"`
	Memory            int `json:"memory"`
	Checker_output    struct {
		Message string
		Verdict string
	} `json:"checker_output"`
}
