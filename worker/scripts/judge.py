import os
import sys
import subprocess
import json
import time
import resource
import tempfile

problem = sys.argv[1]
exec_path = sys.argv[2]
lang = sys.argv[3]
in_file = sys.argv[4]
out_file = sys.argv[5]
mem_limit = int(sys.argv[6]) * 1024  # Convert MB to KB
time_limit = int(sys.argv[7])
CHECKER_PATH = sys.argv[8]

STACK_SIZE = 268435456  # 256 MB

SIGNAL_MAPPING = {
    1: "SIGHUP",
    2: "SIGINT",
    3: "SIGQUIT",
    4: "SIGILL",
    5: "SIGTRAP",
    6: "SIGABRT",
    7: "SIGBUS",
    8: "SIGFPE",
    9: "SIGKILL",
    10: "SIGUSR1",
    11: "SIGSEGV",
    12: "SIGUSR2",
    13: "SIGPIPE",
    14: "SIGALRM",
    15: "SIGTERM",
    16: "SIGSTKFLT",
    17: "SIGCHLD",
    18: "SIGCONT",
    19: "SIGSTOP",
    20: "SIGTSTP",
    21: "SIGTTIN",
    22: "SIGTTOU",
    23: "SIGURG",
    24: "SIGXCPU",
    25: "SIGXFSZ",
    26: "SIGVTALRM",
    27: "SIGPROF",
    28: "SIGWINCH",
    29: "SIGIO",
    30: "SIGPWR",
    31: "SIGSYS",
}

user_out_file = tempfile.mktemp()


def apply_constraints(time_limit, stack_size):
    resource.setrlimit(resource.RLIMIT_CPU, (time_limit, time_limit))
    resource.setrlimit(resource.RLIMIT_STACK, (stack_size, stack_size))


def execute_submission(
    command, time_limit, mem_limit, stack_size, input_file, output_file
):
    start_time = time.time()

    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        process = subprocess.Popen(
            command,
            stdin=infile,
            stdout=outfile,
            stderr=subprocess.DEVNULL,
            shell=False,
            preexec_fn=lambda: apply_constraints(time_limit, stack_size),
        )
        pid = process.pid
        memKill = subprocess.Popen(
            [f"./killif.sh {str(pid)} {mem_limit}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            shell=True,
        )
        process.communicate()
        end_time = time.time()
        mem_used = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        memKill.kill()

        return process.returncode, end_time - start_time, mem_used


def command_exec_wrapper(command_str: str):
    command = command_str.split(" ")
    exit_code, cpu_time, memory_used = execute_submission(
        command,
        time_limit,
        mem_limit,
        STACK_SIZE,
        in_file,
        user_out_file,
    )
    return exit_code, cpu_time, memory_used


if __name__ == "__main__":
    match lang:
        case "C" | "C++ 11" | "C++ 14" | "C++ 17" | "C++ 20" | "Golang" | "Rust":
            exit_code, execution_time, mem_used = command_exec_wrapper(
                f"aa-exec -p bin_jail {exec_path}"
            )
        case _:
            print("Language not supported")
            exit(1)

    # Judging
    checker_output = {}
    if exit_code == 0:
        checker_output = subprocess.run(
            [CHECKER_PATH, user_out_file, out_file], capture_output=True, text=True
        )
        checker_exit_code = checker_output.returncode
        checker_output = json.loads(checker_output.stdout.strip())
    else:
        checker_exit_code = 0
        if exit_code == -9:
            checker_output = {
                "verdict": "TLE",
                "message": f"Your program exceeded the {time_limit}s run-time constraint",
            }
        elif exit_code == -15:
            checker_output = {
                "verdict": "MLE",
                "message": f"Your program exceeded the {mem_limit} KB of run-time memory constraint",
            }
        else:
            checker_output = {
                "verdict": "RE",
                "message": f"Your program crashed with an exit code of: {exit_code} ({SIGNAL_MAPPING.get(-exit_code, 'Unknown')})",
            }

    time_ms = int(execution_time * 1000)

    os.remove(user_out_file)

    json_data = {
        "program_exit_code": exit_code,
        "checker_exit_code": checker_exit_code,
        "memory": mem_used,
        "cpu_time": time_ms,
        "checker_output": checker_output,
    }
    print(json.dumps(json_data, indent=4))
