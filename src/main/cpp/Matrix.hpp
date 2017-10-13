class Matrix {
private:
  int width;
  int height;
  int stride;
  std::vector<float> elements;

public:
  Matrix(int x_size, int y_size) {
    height = x_size;
    width = y_size;

    stride = width;
    elements = std::vector<float>(height * width, 0);
  }

  void setElement(int x, int y, float val) {
    if (y < width && x < height) {
      elements.at(x * stride + y) = val;
    }
  }

  void saveElements(std::string archive_file) {

    std::ofstream ofs(archive_file, std::ios::out | std::ios::binary);
    boost::archive::binary_oarchive oa(ofs);

    oa << elements;
    ofs.close();
  }

  void loadElements(std::string archive_file) {

    elements.clear();
    std::ifstream ifs(archive_file, std::ios::in | std::ifstream::binary);
    boost::archive::binary_iarchive ia(ifs);

    ia >> elements;
    ifs.close();
  }

};
