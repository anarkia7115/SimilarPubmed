#include <sys/inotify.h>
#include <signal.h>

#define EVENT_SIZE    ( sizeof ( struct inotify_event ) ) 
#define EVENT_BUF_LEN ( 1024 * ( EVENT_SIZE + NAME_MAX + 1 ))
#define WATCH_FLAGS   ( IN_CREATE | IN_DELETE )

static bool run = true;

void sig_callback(int sig) {
  run = false;
}

class Watch {
  struct wd_elem {
    int pd;
    string name;
    bool operator() (const wd_elem &l, cont wd_elem &r) const
      { return l.pd < r.pd ? true : l.pd == r.pd && l.name < r.name ? true : false; }
  };
  map<int, wd_elem> watch;
  map<wd_elem, int, wd_elem> rwatch;
public:
  void insert( int pd, cont string &name, int wd) {
    wd_elem elem = {pd, name};
    watch[wd] = elem;
    rwatch[elem] = wd;
  }

  string erase(int pd, const string &name, int *wd) {
    wd_elem pelem = {pd, name};
    *wd = rwatch[pelem];
    rwatch.erase(*wd);
    const wd_elem &elem = watch[*wd];
    string dir = elem.name;
    watch.erase(*wd);
    return dir;
  }

  int get(int pd, string name) {
    wd_elem elem = {pd, name};
    return rwatch[elem];
  }

  void cleanup(int fd){
    for (map<int, wd_elem>::iterator wi = watch.begin(); wi != watch.end(); wi++) {
      inotify_rm_watch(fd, wi->first);
      watch.erase(wi);
    }
    rwatch.clear();
  }
  void stats() {
    cout << "numberof watches=" << watch.size() << " & reverse watches=" << rwatch.size() << endl;
  }
};

int main() {
  Watch watch;

  fd_set watch_set;

  char buffer[EVENT_BUF_LEN];
  string current_dir, new_dir;
  int total_file_events = 0;
  int total_dir_events = 0;

  signal(SIGINT, sig_callback);

#ifdef IN_NONBLOCK
  int fd = inotify_init1(IN_NONBLOCK);
#else
  int fd = inotify_init();
#endif

  if (fd < 0) {
    perror("inotify_init");
  }

  FD_ZERO(&watch_set);
  FD_SET(fd, &watch_set);

  const char *root = "./tmp";
  int wd = inotify_add_watch(fd, root, WATCH_FLAGS);

  watch.insert(-1, root, wd);

  while (run) {

    select(fd+1, &watch_set, NULL, NULL, NULL);

    int length = read(fd, buffer, EVENT_BUF_LEN);
    if(length < 0) {
      perror("read");
    }

    for (int i=0; i<length;) {

      struct inotify_event *event = (struct inotify_event *) &buffer[i];

      if (event->wd == -1) {
        printf("Overflow\n");
      }
    }
  }

}
