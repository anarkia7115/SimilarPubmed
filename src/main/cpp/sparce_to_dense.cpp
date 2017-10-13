#include <set>
#include "TermPwMat.hpp"
#include "PmidTwMat.hpp"
#include "Matrix.hpp"

void errprint(std::string msg) {
  std::cerr << msg << std::endl;
}

int main() {
  std::string pt_file = "./data/pmidTwMat/part-00000";
  std::string td_file = "./data/termPwMat.txt";
  std::string termPwMap_archive_file = "./data/termPwMat_archive.dat";
  std::string td_mat_archive_file = "./data/td_mat_archive.dat";
  std::string pmids_file = "./data/pmids.txt";
  std::string term_ids_file = "./data/term_ids.txt";

  //PmidTwMat pmidTwMat(pt_file);
  TermPwMat termPw;
  termPw.loadMat(termPwMap_archive_file);
  auto termPwMap = termPw.getMat();
  errprint("map loaded!");

  //termPwMat.loadMat(termPwMap_archive_file);
  //termPw.savePmids(pmids_file);
  auto pmid_map = termPw.loadPmids(pmids_file);
  errprint("pmids loaded! ");

  //std::cerr << "pmid_size: " << pmid_vec.size() << std::endl;

}
