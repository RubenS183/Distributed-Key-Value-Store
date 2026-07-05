// Crash-and-restart test: proves WAL durability against a real SIGKILL, not
// a clean shutdown. A child process is forked to perform a sequence of
// put()s; each is acknowledged to the parent over a pipe only after put()
// has returned (i.e. only after the WAL record is fsynced). The parent reads
// exactly N acks, then SIGKILLs the child — by construction (see below) the
// child is guaranteed to be blocked and unable to make further progress at
// that point, so this test has no timing race: exactly N puts are durable,
// and nothing beyond them was ever attempted.
#include <catch2/catch_test_macros.hpp>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>

#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::Store;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_crash_test_" + tag + "_" +
             to_string(chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TempDir() {
    error_code ec;
    fs::remove_all(path_, ec);
  }
  string string() const { return path_.string(); }

 private:
  fs::path path_;
};

}  // namespace

TEST_CASE("crash-and-restart: SIGKILL after N acknowledged writes loses nothing and gains nothing",
          "[wal][crash]") {
  TempDir dir("sigkill");
  constexpr int kTotalKeys = 20;
  constexpr int kKillAfter = 7;  // puts guaranteed acknowledged (fsynced) before SIGKILL

  int ack_pipe[2];
  int cont_pipe[2];
  REQUIRE(pipe(ack_pipe) == 0);
  REQUIRE(pipe(cont_pipe) == 0);

  pid_t pid = fork();
  REQUIRE(pid >= 0);

  if (pid == 0) {
    // Child: no Catch2 assertion macros past this point — they aren't
    // fork-safe (shared reporter state), so failures here would corrupt
    // the parent's test output. _exit(), not exit(), so we don't run any
    // shared atexit/static-destructor state twice.
    ::close(ack_pipe[0]);
    ::close(cont_pipe[1]);

    WriteAheadLog wal(dir.string());
    Store store(&wal);

    for (int i = 0; i < kTotalKeys; ++i) {
      string key = "key" + to_string(i);
      string value = "value" + to_string(i);
      store.put(key, value);  // does not return until the WAL record is fsynced

      char ack = 1;
      ssize_t written = ::write(ack_pipe[1], &ack, 1);
      (void)written;

      if (i == kKillAfter - 1) {
        // Parent never writes to cont_pipe, so this blocks forever —
        // guaranteeing the child cannot start put #kKillAfter before the
        // parent's SIGKILL arrives, regardless of scheduling.
        char buf;
        ssize_t n = ::read(cont_pipe[0], &buf, 1);
        (void)n;
      }
    }
    _exit(0);
  }

  // Parent.
  ::close(ack_pipe[1]);
  ::close(cont_pipe[0]);

  for (int i = 0; i < kKillAfter; ++i) {
    char ack = 0;
    ssize_t n = ::read(ack_pipe[0], &ack, 1);
    REQUIRE(n == 1);
  }
  // The child has now completed and fsynced exactly kKillAfter puts and is
  // blocked waiting on cont_pipe, unable to proceed further.
  REQUIRE(kill(pid, SIGKILL) == 0);

  int status = 0;
  REQUIRE(waitpid(pid, &status, 0) == pid);
  REQUIRE(WIFSIGNALED(status));
  REQUIRE(WTERMSIG(status) == SIGKILL);

  ::close(ack_pipe[0]);
  ::close(cont_pipe[1]);

  // Restart: fresh WAL + Store over the same directory, exactly as the real
  // server would do on process restart after a crash.
  WriteAheadLog wal2(dir.string());
  Store store2(&wal2);
  size_t replayed = store2.recover_from_wal();

  REQUIRE(replayed == static_cast<size_t>(kKillAfter));
  for (int i = 0; i < kKillAfter; ++i) {
    string key = "key" + to_string(i);
    string expected = "value" + to_string(i);
    auto value = store2.get(key);
    REQUIRE(value.has_value());
    REQUIRE(*value == expected);
  }
  for (int i = kKillAfter; i < kTotalKeys; ++i) {
    string key = "key" + to_string(i);
    REQUIRE_FALSE(store2.get(key).has_value());
  }
}
