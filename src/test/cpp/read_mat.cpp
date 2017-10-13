#include "TermPwMat.hpp"

int main(int argc, char* argv[]) {

  // read args
  if (argc < 3) {
      cout 
          << "not enough arguments!\n"
          << "./calcScores <mat_txt_file> <mat_archive_file>"
          << endl;
      return -1;
  }

  TermPwMat termPwMat;
  //std::string td_file = "./data/termPwMat.txt";
  //std::string archive_file = "./data/termPwMat_archive.dat";

  std::string td_file(argv[1]);
  std::string archive_file(argv[2]);

  TermPwMat.readMat(td_file);

  // save data to archive
  TermPwMat.saveMat(archive_file);

  // test read
  //termPwMat.loadMat(archive_file);
  //auto mat = termPwMat.getMat();

  //std::cerr << "mat size: " << mat.size() << std::endl;
}
