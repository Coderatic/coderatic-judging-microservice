#!/bin/bash

lang=$1
src=$2
bin_name=$src
code_path="../cache/code"

case "$lang" in
"C++ 11" | "C++ 14" | "C++ 17" | "C++ 20")
	g++ "$code_path/$src".cpp -static -O2 -o "$code_path"/"$bin_name"
	;;
c)
	gcc "$code_path/$src".c -static -o "$code_path"/"$bin_name"
	;;
Rust)
	rustc "$code_path/$src".rs --crate-type=bin -C prefer-dynamic=no -C target-feature=+crt-static -o "$code_path"/"$bin_name"
	;;
Golang)
	go build --ldflags '-extldflags "-static"' -o "$code_path"/"$bin_name" "$code_path/$src".go
	;;
*)
	echo "Language not supported"
	exit 1
	;;
esac
