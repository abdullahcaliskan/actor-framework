/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2015                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#define CAF_SUITE spawn
#include "caf/test/unit_test.hpp"

#include <stack>
#include <atomic>
#include <chrono>
#include <iostream>
#include <functional>

#include "caf/all.hpp"

using namespace std;
using namespace caf;

namespace {

std::atomic<long> s_max_actor_instances;
std::atomic<long> s_actor_instances;

using a_atom = atom_constant<atom("a")>;
using b_atom = atom_constant<atom("b")>;
using c_atom = atom_constant<atom("c")>;
using abc_atom = atom_constant<atom("abc")>;
using name_atom = atom_constant<atom("name")>;


void inc_actor_instances() {
  long v1 = ++s_actor_instances;
  long v2 = s_max_actor_instances.load();
  while (v1 > v2) {
    s_max_actor_instances.compare_exchange_strong(v2, v1);
  }
}

void dec_actor_instances() {
  --s_actor_instances;
}

class event_testee : public sb_actor<event_testee> {
 public:
  event_testee();
  ~event_testee();

  behavior wait4string;
  behavior wait4float;
  behavior wait4int;

  behavior& init_state = wait4int;
};

event_testee::event_testee() {
  inc_actor_instances();
  wait4string.assign(
    [=](const std::string&) {
      become(wait4int);
    },
    [=](get_atom) {
      return "wait4string";
    }
  );
  wait4float.assign(
    [=](float) {
      become(wait4string);
    },
    [=](get_atom) {
      return "wait4float";
    }
  );
  wait4int.assign(
    [=](int) {
      become(wait4float);
    },
    [=](get_atom) {
      return "wait4int";
    }
  );
}

event_testee::~event_testee() {
  dec_actor_instances();
}

// quits after 5 timeouts
actor spawn_event_testee2(actor parent) {
  struct impl : event_based_actor {
    actor parent;
    impl(actor parent_actor) : parent(parent_actor) {
      inc_actor_instances();
    }
    ~impl() {
      dec_actor_instances();
    }
    behavior wait4timeout(int remaining) {
      return {
        after(chrono::milliseconds(1)) >> [=] {
          CAF_MESSAGE(CAF_ARG(remaining));
          if (remaining == 1) {
            send(parent, ok_atom::value);
            quit();
          }
          else become(wait4timeout(remaining - 1));
        }
      };
    }
    behavior make_behavior() override {
      return wait4timeout(5);
    }
  };
  return spawn<impl>(parent);
}

class testee_actor : public blocking_actor {
 public:
  testee_actor();
  ~testee_actor();
  void act() override;

 private:
  void wait4string();
  void wait4float();
};

testee_actor::testee_actor() {
  inc_actor_instances();
}

testee_actor::~testee_actor() {
  dec_actor_instances();
}

void testee_actor::act() {
  receive_loop (
    [&](int) {
      wait4float();
    },
    [&](get_atom) {
      return "wait4int";
    }
  );
}

void testee_actor::wait4string() {
  bool string_received = false;
  do_receive (
    [&](const string&) {
      string_received = true;
    },
    [&](get_atom) {
      return "wait4string";
    }
  )
  .until([&] { return string_received; });
}

void testee_actor::wait4float() {
  bool float_received = false;
  do_receive (
    [&](float) {
      float_received = true;
    },
    [&](get_atom) {
      return "wait4float";
    }
  )
  .until([&] { return float_received; });
  wait4string();
}

// self->receives one timeout and quits
class testee1 : public event_based_actor {
 public:
  testee1();
  ~testee1();
  behavior make_behavior() override;
};

testee1::testee1() {
  inc_actor_instances();
}

testee1::~testee1() {
  dec_actor_instances();
}

behavior testee1::make_behavior() {
  return {
    after(chrono::milliseconds(10)) >> [=] {
      unbecome();
    }
  };
}

class echo_actor : public event_based_actor {
 public:
  echo_actor();
  ~echo_actor();
  behavior make_behavior() override;
};

echo_actor::echo_actor() {
  inc_actor_instances();
}

echo_actor::~echo_actor() {
  dec_actor_instances();
}

behavior echo_actor::make_behavior() {
  return {
    others >> [=]() -> message {
      quit(exit_reason::normal);
      return current_message();
    }
  };
}

class simple_mirror : public event_based_actor {
 public:
  simple_mirror();
  ~simple_mirror();
  behavior make_behavior() override;
};

simple_mirror::simple_mirror() {
  inc_actor_instances();
}

simple_mirror::~simple_mirror() {
  dec_actor_instances();
}

behavior simple_mirror::make_behavior() {
  return {
    others >> [=] {
      CAF_MESSAGE("simple_mirror: return current message");
      return current_message();
    }
  };
}

behavior high_priority_testee(event_based_actor* self) {
  self->send(self, b_atom::value);
  self->send(message_priority::high, self, a_atom::value);
  // 'a' must be self->received before 'b'
  return {
    [=](b_atom) {
      CAF_TEST_ERROR("received 'b' before 'a'");
      self->quit();
    },
    [=](a_atom) {
      CAF_MESSAGE("received \"a\" atom");
      self->become (
        [=](b_atom) {
          CAF_MESSAGE("received \"b\" atom, about to quit");
          self->quit();
        },
        others >> [&] {
          CAF_TEST_ERROR("Unexpected message: "
                         << to_string(self->current_message()));
        }
      );
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    }
  };
}

struct high_priority_testee_class : event_based_actor {
  behavior make_behavior() override {
    return high_priority_testee(this);
  }
};

struct master : event_based_actor {
  behavior make_behavior() override {
    return (
      [=](ok_atom) {
        CAF_MESSAGE("master: received done");
        quit(exit_reason::user_shutdown);
      }
    );
  }
};

struct slave : event_based_actor {

  slave(actor master_actor) : master{master_actor} { }

  behavior make_behavior() override {
    link_to(master);
    trap_exit(true);
    return {
      [=](const exit_msg& msg) {
        CAF_MESSAGE("slave: received exit message");
        quit(msg.reason);
      },
      others >> [&] {
        CAF_TEST_ERROR("Unexpected message: " << to_string(current_message()));
      }
    };
  }

  actor master;

};

class counting_actor : public event_based_actor {
 public:
  counting_actor();
  ~counting_actor();
  behavior make_behavior() override;
};

counting_actor::counting_actor() {
  inc_actor_instances();
}

counting_actor::~counting_actor() {
  dec_actor_instances();
}

behavior counting_actor::make_behavior() {
  for (int i = 0; i < 100; ++i) {
    send(this, ok_atom::value);
  }
  CAF_CHECK_EQUAL(mailbox().count(), 100);
  for (int i = 0; i < 100; ++i) {
    send(this, ok_atom::value);
  }
  CAF_CHECK_EQUAL(mailbox().count(), 200);
  return {};
}

struct fixture {
  ~fixture() {
    await_all_actors_done();
    shutdown();
    CAF_CHECK_EQUAL(s_actor_instances.load(), 0);
    CAF_MESSAGE("max. # of actor instances: " << s_max_actor_instances.load());
  }
};

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(atom_tests, fixture)

CAF_TEST(count_mailbox) {
  spawn<counting_actor>();
}

CAF_TEST(send_to_self) {
  scoped_actor self;
  self->send(self, 1, 2, 3, true);
  self->receive(on(1, 2, 3, true) >> [] { });
  self->send(self, message{});
  self->receive(on() >> [] { });
}

CAF_TEST(detached_actors_and_schedulued_actors) {
  scoped_actor self;
  // check whether detached actors and scheduled actors interact w/o errors
  auto m = spawn<master, detached>();
  spawn<slave>(m);
  spawn<slave>(m);
  self->send(m, ok_atom::value);
}

CAF_TEST(self_receive_with_zero_timeout) {
  scoped_actor self;
  self->receive(
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    },
    after(chrono::seconds(0)) >> [] { /* mailbox empty */ }
  );
}

CAF_TEST(mirror) {
  scoped_actor self;
  auto mirror = self->spawn<simple_mirror, monitored>();
  self->send(mirror, "hello mirror");
  self->receive (
    on("hello mirror") >> [] {
      CAF_MESSAGE("received \"hello mirror\"");
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    }
  );
  self->send_exit(mirror, exit_reason::user_shutdown);
  self->receive (
    [&](const down_msg& dm) {
      if (dm.reason == exit_reason::user_shutdown) {
        CAF_MESSAGE("received `down_msg`");
      }
      else {
        CAF_TEST_ERROR("Unexpected message: "
                       << to_string(self->current_message()));
      }
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message"
                     << to_string(self->current_message()));
    }
  );
}

CAF_TEST(detached_mirror) {
  scoped_actor self;
  auto mirror = self->spawn<simple_mirror, monitored+detached>();
  self->send(mirror, "hello mirror");
  self->receive (
    on("hello mirror") >> [] {
      CAF_MESSAGE("received \"hello mirror\"");
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    }
  );
  self->send_exit(mirror, exit_reason::user_shutdown);
  self->receive (
    [&](const down_msg& dm) {
      if (dm.reason == exit_reason::user_shutdown) {
        CAF_MESSAGE("received `down_msg`");
      }
      else {
        CAF_TEST_ERROR("Unexpected message: "
                       << to_string(self->current_message()));
      }
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    }
  );
}

CAF_TEST(priority_aware_mirror) {
  scoped_actor self;
  auto mirror = self->spawn<simple_mirror, monitored + priority_aware>();
  CAF_MESSAGE("spawned mirror");
  self->send(mirror, "hello mirror");
  self->receive (
    on("hello mirror") >> [] {
      CAF_MESSAGE("received \"hello mirror\"");
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: " << to_string(self->current_message()));
    }
  );
  self->send_exit(mirror, exit_reason::user_shutdown);
  self->receive (
    [&](const down_msg& dm) {
      if (dm.reason == exit_reason::user_shutdown) {
        CAF_MESSAGE("received `down_msg`");
      }
      else {
        CAF_TEST_ERROR("Unexpected message: "
                       << to_string(self->current_message()));
      }
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    }
  );
}

CAF_TEST(echo_actor) {
  scoped_actor self;
  auto mecho = spawn<echo_actor>();
  self->send(mecho, "hello echo");
  self->receive(
    on("hello echo") >> [] { CAF_MESSAGE("received \"hello echo\""); },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: " << to_string(self->current_message()));
    }
  );
}

CAF_TEST(delayed_send) {
  scoped_actor self;
  self->delayed_send(self, chrono::milliseconds(1), 1, 2, 3);
  self->receive(on(1, 2, 3) >> [] { });
}

CAF_TEST(delayed_spawn) {
  scoped_actor self;
  self->receive(after(chrono::milliseconds(1)) >> [] { });
  spawn<testee1>();
}

CAF_TEST(spawn_event_testee2) {
  scoped_actor self;
  spawn_event_testee2(self);
  self->receive(
    [](ok_atom) {
      CAF_MESSAGE("Received 'ok'");
    }
  );
}

CAF_TEST(chopsticks) {
  struct chopstick : event_based_actor {
    behavior make_behavior() override {
      return available;
    }

    behavior taken_by(actor whom) {
      return {
        [=](get_atom) {
          return error_atom::value;
        },
        on(put_atom::value, whom) >> [=]() {
          become(available);
        }
      };
    }

    chopstick() {
      inc_actor_instances();
      available.assign(
        [=](get_atom, actor whom) -> atom_value {
          become(taken_by(whom));
          return ok_atom::value;
        }
      );
    }

    ~chopstick() {
      dec_actor_instances();
    }

    behavior available;
  };
  scoped_actor self;
  auto cstk = spawn<chopstick>();
  self->send(cstk, get_atom::value, self);
  self->receive(
    [&](ok_atom) {
      self->send(cstk, put_atom::value, self);
      self->send_exit(cstk, exit_reason::kill);
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: " <<
                     to_string(self->current_message()));
    }
  );
}

CAF_TEST(sync_sends) {
  scoped_actor self;
  auto sync_testee = spawn<blocking_api>([](blocking_actor* s) {
    s->receive (
      on("hi", arg_match) >> [&](actor from) {
        s->sync_send(from, "whassup?", s).await(
          on_arg_match >> [&](const string& str) -> string {
            CAF_CHECK(s->current_sender() != nullptr);
            CAF_CHECK_EQUAL(str, "nothing");
            return "goodbye!";
          },
          after(chrono::minutes(1)) >> [] {
            CAF_TEST_ERROR("Error in unit test.");
            abort();
          }
        );
      },
      others >> [&] {
        CAF_TEST_ERROR("Unexpected message: "
                       << to_string(s->current_message()));
      }
    );
  });
  self->monitor(sync_testee);
  self->send(sync_testee, "hi", self);
  self->receive (
    on("whassup?", arg_match) >> [&](actor other) -> std::string {
      CAF_MESSAGE("received \"whassup?\" message");
      // this is NOT a reply, it's just an asynchronous message
      self->send(other, "a lot!");
      return "nothing";
    }
  );
  self->receive (
    on("goodbye!") >> [] { CAF_MESSAGE("Received \"goodbye!\""); },
    after(chrono::seconds(1)) >> [] { CAF_TEST_ERROR("Unexpected timeout"); }
  );
  self->receive (
    [&](const down_msg& dm) {
      CAF_CHECK_EQUAL(dm.reason, exit_reason::normal);
      CAF_CHECK_EQUAL(dm.source, sync_testee);
    }
  );
  self->await_all_other_actors_done();
  self->sync_send(sync_testee, "!?").await(
    on<sync_exited_msg>() >> [] {
      CAF_MESSAGE("received `sync_exited_msg`");
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    },
    after(chrono::microseconds(1)) >> [] {
      CAF_TEST_ERROR("Unexpected timeout");
    }
  );
}

CAF_TEST(inflater) {
  scoped_actor self;
  struct inflater : public event_based_actor {
   public:
    inflater(string name, actor buddy)
        : m_name(std::move(name)),
          m_buddy(std::move(buddy)) {
      inc_actor_instances();
    }
    ~inflater() {
      dec_actor_instances();
    }
    behavior make_behavior() override {
      return {
        [=](int n, const string& str) {
          send(m_buddy, n * 2, str + " from " + m_name);
        },
        [=](ok_atom) {
          quit();
        }
      };
    }
   private:
    string m_name;
    actor m_buddy;
  };
  auto joe = spawn<inflater>("Joe", self);
  auto bob = spawn<inflater>("Bob", joe);
  self->send(bob, 1, "hello actor");
  self->receive (
    on(4, "hello actor from Bob from Joe") >> [] {
      CAF_MESSAGE("received message");
    },
    others >> [&] {
      CAF_TEST_ERROR("Unexpected message: "
                     << to_string(self->current_message()));
    }
  );
  // kill joe and bob
  auto ok_message = make_message(ok_atom::value);
  anon_send(joe, ok_message);
  anon_send(bob, ok_message);
}

CAF_TEST(kr34t0r) {
  class kr34t0r : public event_based_actor {
   public:
    kr34t0r(string name, actor pal)
        : m_name(std::move(name)),
          m_pal(std::move(pal)) {
      inc_actor_instances();
    }
    ~kr34t0r() {
      dec_actor_instances();
    }
    behavior make_behavior() override {
      if (m_name == "Joe" && m_pal == invalid_actor) {
        m_pal = spawn<kr34t0r>("Bob", this);
      }
      return {
        others >> [=] {
          // forward message and die
          send(m_pal, current_message());
          quit();
        }
      };
    }
    void on_exit() {
      m_pal = invalid_actor; // break cycle
    }

   private:
    string m_name;
    actor m_pal;
  };
  scoped_actor self;
  auto joe_the_second = spawn<kr34t0r>("Joe", invalid_actor);
  self->send(joe_the_second, ok_atom::value);
}

CAF_TEST(function_spawn) {
  scoped_actor self;
  auto f = [](const string& name) -> behavior {
    return (
      [name](get_atom) {
        return std::make_tuple(name_atom::value, name);
      }
    );
  };
  auto a1 = spawn(f, "alice");
  auto a2 = spawn(f, "bob");
  self->send(a1, get_atom::value);
  self->receive (
    [&](name_atom, const string& name) {
      CAF_CHECK_EQUAL(name, "alice");
    }
  );
  self->send(a2, get_atom::value);
  self->receive (
    [&](name_atom, const string& name) {
      CAF_CHECK_EQUAL(name, "bob");
    }
  );
  self->send_exit(a1, exit_reason::user_shutdown);
  self->send_exit(a2, exit_reason::user_shutdown);
}

using typed_testee = typed_actor<replies_to<abc_atom>::with<std::string>>;

typed_testee::behavior_type testee() {
  return {
    [](abc_atom) {
      CAF_MESSAGE("received 'abc'");
      return "abc";
    }
  };
}

CAF_TEST(typed_await) {
  scoped_actor self;
  auto x = spawn_typed(testee);
  self->sync_send(x, abc_atom::value).await(
    [](const std::string& str) {
      CAF_CHECK_EQUAL(str, "abc");
    }
  );
  self->send_exit(x, exit_reason::user_shutdown);
}

// tests attach_functor() inside of an actor's constructor
CAF_TEST(constructor_attach) {
  class testee : public event_based_actor {
   public:
    testee(actor buddy) : m_buddy(buddy) {
      attach_functor([=](uint32_t reason) {
        send(buddy, ok_atom::value, reason);
      });
    }

    behavior make_behavior() {
      return {
        others >> [=] {
          CAF_TEST_ERROR("Unexpected message: "
                         << to_string(current_message()));
        }
      };
    }

    void on_exit() {
      m_buddy = invalid_actor;
    }

   private:
    actor m_buddy;
  };
  class spawner : public event_based_actor {
   public:
    spawner() : m_downs(0) {
      // nop
    }
    behavior make_behavior() {
      trap_exit(true);
      m_testee = spawn<testee, monitored>(this);
      return {
        [=](const down_msg& msg) {
          CAF_CHECK_EQUAL(msg.reason, exit_reason::user_shutdown);
          if (++m_downs == 2) {
            quit(msg.reason);
          }
        },
        [=](ok_atom, uint32_t reason) {
          CAF_CHECK_EQUAL(reason, exit_reason::user_shutdown);
          if (++m_downs == 2) {
            quit(reason);
          }
        },
        others >> [=] {
          forward_to(m_testee);
        }
      };
    }

    void on_exit() {
      m_testee = invalid_actor;
    }

   private:
    int m_downs;
    actor m_testee;
  };
  anon_send_exit(spawn<spawner>(), exit_reason::user_shutdown);
}

namespace {

class exception_testee : public event_based_actor {
 public:
  exception_testee() {
    set_exception_handler([](const std::exception_ptr&) -> optional<uint32_t> {
      return exit_reason::user_defined + 2;
    });
  }
  behavior make_behavior() override {
    return {
      others >> [] {
        throw std::runtime_error("whatever");
      }
    };
  }
};

} // namespace <anonymous>

CAF_TEST(custom_exception_handler) {
  auto handler = [](const std::exception_ptr& eptr) -> optional<uint32_t> {
    try {
      std::rethrow_exception(eptr);
    }
    catch (std::runtime_error&) {
      return exit_reason::user_defined;
    }
    catch (...) {
      // "fall through"
    }
    return exit_reason::user_defined + 1;
  };
  scoped_actor self;
  auto testee1 = self->spawn<monitored>([=](event_based_actor* eb_self) {
    eb_self->set_exception_handler(handler);
    throw std::runtime_error("ping");
  });
  auto testee2 = self->spawn<monitored>([=](event_based_actor* eb_self) {
    eb_self->set_exception_handler(handler);
    throw std::logic_error("pong");
  });
  auto testee3 = self->spawn<exception_testee, monitored>();
  self->send(testee3, "foo");
  // receive all down messages
  auto i = 0;
  self->receive_for(i, 3)(
    [&](const down_msg& dm) {
      if (dm.source == testee1) {
        CAF_CHECK_EQUAL(dm.reason, exit_reason::user_defined);
      }
      else if (dm.source == testee2) {
        CAF_CHECK_EQUAL(dm.reason, exit_reason::user_defined + 1);
      }
      else if (dm.source == testee3) {
        CAF_CHECK_EQUAL(dm.reason, exit_reason::user_defined + 2);
      }
      else {
        CAF_CHECK(false); // report error
      }
    }
  );
}

CAF_TEST(kill_the_immortal) {
  auto wannabe_immortal = spawn([](event_based_actor* self) -> behavior {
    self->trap_exit(true);
    return {
      others >> [=] {
        CAF_TEST_ERROR("Unexpected message: "
                       << to_string(self->current_message()));
      }
    };
  });
  scoped_actor self;
  self->monitor(wannabe_immortal);
  self->send_exit(wannabe_immortal, exit_reason::kill);
  self->receive(
    [&](const down_msg& dm) {
      CAF_CHECK(dm.reason == exit_reason::kill);
      CAF_CHECK(dm.source == wannabe_immortal);
    }
  );
}

CAF_TEST(exit_reason_in_scoped_actor) {
  scoped_actor self;
  self->spawn<linked>([]() -> behavior { return others >> [] {}; });
  self->planned_exit_reason(exit_reason::user_defined);
}

CAF_TEST_FIXTURE_SCOPE_END()
