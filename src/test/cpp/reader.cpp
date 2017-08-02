#include <iostream>
#include <fstream>
#include <string>
#include <vector>

int main(int argc, char** argv) {

  // read args
  if (argc < 2) {
    std::cerr 
      << "not enough arguments!\n"
      << "./calcScores <input_file>"
      << std::endl;
    return -1;
  }

  std::string in_file(argv[1]);
  std::ifstream ifs(in_file.c_str());

  std::string line;
  int lnr = 0;
  std::vector<std::string> line_vec;
  std::cerr << "start to read big file..." << std::endl;
  while (getline(ifs, line) ) {
    if (lnr % 10000 == 0) {
      std::cerr << lnr << "lines read." << std::endl;
    }
    //line_vec.push_back(line);
    lnr ++;
  }

  return 0;
}
