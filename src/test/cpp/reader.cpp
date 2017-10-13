#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>

void read_file_thread(std::string file_path, std::vector<std::string> &line_vec);
void thread_func(std::ifstream& ifs, std::vector<std::string> & line_vec);
void push_file_path(std::vector<std::string>& fp_vec);

std::mutex mtx;
int main(int argc, char** argv) {

  // read args
  /*
  if (argc < 2) {
    std::cerr 
      << "not enough arguments!\n"
      << "./calcScores <input_file>"
      << std::endl;
    return -1;
  }
  */

  int NUM_THREADS = 8;

  std::string in_file("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/termPwMat.txt");
  std::ifstream ifs(in_file.c_str());

  std::string line;
  int lnr = 0;
  std::vector<std::string> line_vec;
  std::cerr << "start to read big file..." << std::endl;

  // normal reading
  while (getline(ifs, line)) {
    if (lnr % 10000 == 0) {
      std::cerr << lnr << "lines read." << std::endl;
    }
    line_vec.push_back(line);
    lnr ++;
  }

  // read from mat file
  /*
  std::ifstream is("mat.dat");
  is.seekg(0, std::ios_base::end);
  std::size_t size=is.tellg();
  is.seekg(0, std::ios_base::beg);

  std::vector<std::string> v(size / sizeof(std::string));

  is.read((char*) &v[0], size);

  is.close();
  */

  /* write to binary file */
  std::cerr << "writing to mat.dat..." << std::endl;
  std::ofstream fout("mat.dat", std::ios::out | std::ofstream::binary);
  std::cerr << "size of string: " << sizeof(std::string) << std::endl;
  fout.write((char*)&line_vec[0], line_vec.size() * sizeof(std::string));
  fout.close();

  std::cerr << "mat.dat is Done!" << std::endl;

  std::cout << "line_vec.size: " << line_vec.size() << std::endl;
  //std::cout << "line_vec.size: " << line_vec.size() << std::endl;

  return 0;
}

void push_file_path(std::vector<std::string>& fp_vec) {

  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-00");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-01");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-02");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-03");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-04");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-05");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-06");
  fp_vec.push_back("/home/shawn/git/PubmedWordGame/SimilarPubmed/data/splitted_termPwMat/termPwMat-part-07");
}

void read_file_thread(std::string file_path, std::vector<std::string> &line_vec) {

  std::ifstream ifs(file_path);
  std::string line;

  while (getline(ifs, line)) {
    int lnr = line_vec.size();
    if (lnr % 10000 == 0) {
      std::cerr << lnr << "lines read." << std::endl;
    }
    mtx.lock();
    line_vec.push_back(line);
    mtx.unlock();
  }

}
