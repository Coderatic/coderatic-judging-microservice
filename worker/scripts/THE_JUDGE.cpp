#include <iostream>
#include <fstream>
#include <string>
#include <format>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

std::string getOrdinalIndicator(int number) {
    if (number % 100 >= 11 && number % 100 <= 13) {
        return "th";
    }
    switch (number % 10) {
        case 1: return "st";
        case 2: return "nd";
        case 3: return "rd";
        default: return "th";
    }
}

int main(int argc, char* argv[]) {

    json result = {
      {"verdict", nullptr},
      {"message", nullptr},
    };

    if (argc == 2) {
        result["verdict"] = "IE";
        result["message"] = "Didn't receive the solution output file path";
        return -1;
    }

    std::ifstream user_out_file(argv[1]);
    std::ifstream sol_out_file(argv[2]);

    if (!sol_out_file) {
        result["verdict"] = "IE";
        result["message"] = "Failed to open the solution output file for reading.";
        return 1;
    }
    else if(!user_out_file) {
        result["verdict"] = "IE";
        result["message"] = "Failed to open the user output file for reading.";
        return 1;
    }

    std::string sol_out_line;
    std::string user_out_line;
    int line_number = 1;
    bool inner_break = false;
    while (true) {
        do {
            std::getline(user_out_file, user_out_line);
        } while(user_out_line.empty() && !user_out_file.eof());
        do {
            std::getline(sol_out_file, sol_out_line);
        } while(sol_out_line.empty() && !sol_out_file.eof());

        if (user_out_line != sol_out_line) {
            result["verdict"] = "WA";
            result["message"] = std::format("{}{} line differs from the solution output file", line_number, getOrdinalIndicator(line_number));
            break;
        }
        else if(user_out_file.eof() && sol_out_file.eof()) {
            result["verdict"] = "AC";
            result["message"] = std::format("Ok {} lines", line_number);
            break;
        }

        if(user_out_file.eof()) {
          while(std::getline(sol_out_file, sol_out_line)) {
            if(!sol_out_line.empty()) {
              result["verdict"] = "";
              result["message"] = "Your program output less lines than the solution output file";
              inner_break = true;
              break;
            }
          }
          if(user_out_file.eof()) {
              result["verdict"] = "AC";
              result["message"] = std::format("Ok {} line(s)", line_number);
              inner_break = true;
              break;
            }
          if(inner_break) break;
        }

        else if(sol_out_file.eof()) {
          while(std::getline(user_out_file, user_out_line)) {
            if(!user_out_line.empty()) {
              result["verdict"] = "PE";
              result["message"] = "Your program output more lines than the solution output file";
              inner_break = true;
              break;
            }
          }
          if(user_out_file.eof()) {
              result["verdict"] = "AC";
              result["message"] = std::format("Ok {} lines", line_number);
              inner_break = true;
              break;
          }
          if(inner_break) break;

        }
        else
          ++line_number;
    }

if(result["verdict"] == nullptr) {
        result["verdict"] = "AC";
        result["message"] = std::format("Ok {} lines", line_number);
    }

    std::cout << result.dump(4) << std::endl;
    user_out_file.close();
    sol_out_file.close();
    return 0;
}
