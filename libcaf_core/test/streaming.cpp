/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2016                                                  *
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

#include <string>
#include <fstream>
#include <iostream>
#include <iterator>

#include "caf/config.hpp"

#define CAF_SUITE streaming
#include "caf/test/unit_test.hpp"

#include "caf/all.hpp"

using std::cout;
using std::endl;
using std::string;


namespace {

using namespace caf;

template <class F>
using signature_t = typename detail::get_callable_trait<F>::fun_sig;

struct stream_id {
  strong_actor_ptr origin;
  uint64_t nr;
};

bool operator==(const stream_id& x, const stream_id& y) {
  return x.origin == y.origin && x.nr == y.nr;
}

bool operator<(const stream_id& x, const stream_id& y) {
  return x.origin == y.origin ? x.nr < y.nr : x.origin < y.origin;
}

} // namespace <anonymous>

namespace std {
template <>
struct hash<stream_id> {
  size_t operator()(const stream_id& x) const {
    auto tmp = reinterpret_cast<ptrdiff_t>(x.origin.get())
               ^ static_cast<ptrdiff_t>(x.nr);
    return tmp;
  }
};
} // namespace std

namespace {

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_id& x) {
  return f(meta::type_name("stream_id"), x.origin, x.nr);
}

/// Categorizes individual streams.
enum class stream_priority {
  /// Denotes best-effort traffic.
  low,
  /// Denotes traffic with moderate timing requirements.
  normal,
  /// Denotes soft-realtime traffic.
  high
};

std::string to_string(stream_priority x) {
  switch (x) {
    default: return "invalid";
    case stream_priority::low: return "low";
    case stream_priority::normal: return "normal";
    case stream_priority::high: return "high";
  }
}

/// Identifies an unbound sequence of messages.
template <class T>
class stream {
public:
  stream() = default;
  stream(stream&&) = default;
  stream(const stream&) = default;
  stream& operator=(stream&&) = default;
  stream& operator=(const stream&) = default;

  stream(stream_id sid) : id_(std::move(sid)) {
    // nop
  }

  const stream_id& id() const {
    return id_;
  }

  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, stream& x) {
    return f(x.id_);
  }

private:
  stream_id id_;
};

struct stream_msg {
  /// Initiates a stream handshake.
  struct open {
    /// A type-erased stream<T> object for picking the correct message
    /// handler of the receiving actor.
    message token;
    /// A pointer to the previous stage in the pipeline.
    strong_actor_ptr prev_stage;
    /// Configures the priority for stream elements.
    stream_priority priority;
  };

  /// Finalizes a stream handshake and signalizes initial demand.
  struct ok {
    /// Grants credit to the source.
    int32_t initial_demand;
  };

  /// Transmits work items.
  struct batch {
    /// Size of the type-erased vector<T> (released credit).
    int32_t xs_size;
    /// A type-erased vector<T> containing the elements of the batch.
    message xs;
    /// ID of this batch (ascending numbering).
    int64_t id;
  };

  /// Cumulatively acknowledges received batches and signalizes new demand from
  /// a sink to its source.
  struct demand {
    /// Newly available credit.
    int32_t new_capacity;
    /// Cumulative ack ID.
    int64_t acknowledged_id;
  };

  /// Closes a stream.
  struct close {
    // tag type
  };

  /// Propagates fatal errors.
  struct abort {
    error reason;
  };

  struct downstream_failed {
    error reason;
  };

  struct upstream_failed {
    error reason;
  };

  using content_type = variant<open, ok, batch, demand, close, abort,
                               downstream_failed, upstream_failed>;

  stream_id sid;
  content_type content;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::open& x) {
  return f(meta::type_name("open"), x.token, x.priority);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::ok& x) {
  return f(meta::type_name("ok"), x.initial_demand);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::batch& x) {
  return f(meta::type_name("batch"), meta::omittable(), x.xs_size, x.xs);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::demand& x) {
  return f(meta::type_name("demand"), x.new_capacity, x.acknowledged_id);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::close&) {
  return f(meta::type_name("close"));
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg::abort& x) {
  return f(meta::type_name("abort"), x.reason);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f,
                                        stream_msg::downstream_failed& x) {
  return f(meta::type_name("downstream_failed"), x.reason);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f,
                                        stream_msg::upstream_failed& x) {
  return f(meta::type_name("upstream_failed"), x.reason);
}

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_msg& x) {
  return f(meta::type_name("stream_msg"), x.sid, x.content);
}

stream_msg make_stream_open(const stream_id& sid, message token,
                            strong_actor_ptr prev_stage,
                            stream_priority pr = stream_priority::normal) {
  return {sid, stream_msg::open{std::move(token), std::move(prev_stage), pr}};
}

template <class T>
stream_msg make_stream_open(stream<T> token, strong_actor_ptr prev_stage,
                            stream_priority pr = stream_priority::normal) {
  // make_message rejects stream<T>, because stream<T> has special meaning
  // in CAF. However, we still need the token in order to dispatch to the
  // user-defined message handler. This code bypasses make_message
  // for this purpose.
  using storage = detail::tuple_vals<stream<T>>;
  auto ptr = make_counted<storage>(token);
  message msg{detail::message_data::cow_ptr{std::move(ptr)}};
  return {std::move(token.id()),
          stream_msg::open{std::move(msg), std::move(prev_stage), pr}};
}

inline stream_msg make_stream_ok(stream_id sid, int32_t initial_demand) {
  return {std::move(sid), stream_msg::ok{initial_demand}};
}

inline stream_msg make_stream_batch(stream_id sid, int32_t xs_size, message xs,
                                    int64_t batch_id) {
  return {std::move(sid), stream_msg::batch{xs_size, std::move(xs), batch_id}};
}

inline stream_msg make_stream_demand(stream_id sid, int32_t value,
                                     int64_t ack_id) {
  return {std::move(sid), stream_msg::demand{value, ack_id}};
}

inline stream_msg make_stream_close(stream_id sid) {
  return {std::move(sid), stream_msg::close{}};
}

inline stream_msg make_stream_abort(stream_id sid, error reason) {
  return {std::move(sid), stream_msg::abort{std::move(reason)}};
}

} // namespace <anonymous>

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<int>)

namespace {

inline void debug_rec() {
  std::cout << std::endl;
}

template <class T, class... Ts>
void debug_rec(const T& x, const Ts&... xs) {
  std::cout << deep_to_string(x);
  debug_rec(xs...);
}

#define debug(...)                                                             \
  std::cout << "[id: " << self->id() << " line " << __LINE__ << "] ";          \
  debug_rec(__VA_ARGS__)

/// Manages a single stream with any number of down- and upstream actors.
class stream_handler : public ref_counted {
public:
  /// Add a new downstream actor to the stream.
  virtual error add_downstream(stream_id& id, strong_actor_ptr& sink) = 0;

  /// Add credit to a downstream.
  virtual error add_credit(stream_id& id, strong_actor_ptr& sink,
                           int32_t amount) = 0;

  /// Sends data downstream if possible. If `cause` is not `nullptr` then it
  /// is interpreted as the last downstream actor that signaled demand.
  virtual error trigger_send(strong_actor_ptr& cause) = 0;

  /// Add a new upstream actor to the stream.
  virtual error add_upstream(stream_id& id, strong_actor_ptr& source) = 0;

  /// Handles data from an upstream actor.
  virtual error handle_data(stream_id& id, strong_actor_ptr& source,
                            message& batch) = 0;

  /// Closes an upstream.
  virtual error close_upstream(stream_id& id, strong_actor_ptr& source) = 0;

  /// Sends demand upstream if possible. If `cause` is not `nullptr` then it
  /// is interpreted as the last upstream actor that sent data.
  virtual error trigger_demand(strong_actor_ptr& cause) = 0;

  /// Shutdown the stream due to a fatal error.
  virtual void abort(strong_actor_ptr& cause, const error& reason) = 0;

  /// Queries whether this handler is a sink, i.e., does not accept downstream
  /// actors.
  virtual bool is_sink() const {
    return false;
  }

  /// Queries whether this handler is a source, i.e., does not accepts upstream
  /// actors.
  virtual bool is_source() const {
    return false;
  }

  /// Returns a type-erased `stream<T>` as handshake token for downstream
  /// actors. Returns an empty message for sinks.
  virtual message make_output_token(const stream_id&) const {
    return make_message();
  }
};

/// Smart pointer to a stream handler.
/// @relates stream_handler
using stream_handler_ptr = intrusive_ptr<stream_handler>;

/// Mixin for streams without upstreams.
template <class Base, class Subtype>
class no_upstreams : public Base {
public:
  error add_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::cannot_add_upstream;
  }

  error handle_data(stream_id&, strong_actor_ptr&, message&) final {
    CAF_LOG_ERROR("Cannot handle upstream data in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error close_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error trigger_demand(strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot trigger demand in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  bool is_source() const override {
    return true;
  }
};

/// Mixin for streams without downstreams.
template <class Base, class Subtype>
class no_downstreams : public Base {
public:
  error add_downstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add downstream to a stream marked as no-downstreams");
    return sec::cannot_add_downstream;
  }

  error add_credit(stream_id&, strong_actor_ptr&, int32_t) final {
    CAF_LOG_ERROR("Cannot handle downstream demand in a stream marked as no-downstreams");
    return sec::invalid_downstream;
  }

  error trigger_send(strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot send downstream in a stream marked as no-downstream");
    return sec::invalid_downstream;
  }

  bool is_sink() const override {
    return true;
  }

  message make_output_token(const stream_id&) const override {
    return make_message();
  }
};

struct sink {
  int32_t credit = 0;
  int64_t next_batch_id = 0;
  strong_actor_ptr ptr;

  sink(strong_actor_ptr p) : credit(0), next_batch_id(0), ptr(std::move(p)) {
    // nop
  }
};

struct source {
  int32_t assigned_credit = 0;
  strong_actor_ptr ptr;
};


inline bool credit_cmp(const std::pair<const stream_id, sink>& x,
                       const std::pair<const stream_id, sink>& y) noexcept {
  return x.second.credit < y.second.credit;
}


template <class T>
class downstream {
public:
  using sinks_map = std::unordered_map<stream_id, sink>;

  using value_type = typename sinks_map::value_type;

  downstream(local_actor* ptr) : self_(ptr) {
    // nop
  }

  template <class... Ts>
  void push(Ts&&... xs) {
    buf_.emplace_back(std::forward<Ts>(xs)...);
  }

  /// Returns the total available credit for all sinks in O(n).
  int32_t total_credit() const {
    auto add = [](int32_t x, const value_type& y) {
      return x + y.second.credit;
    };
    return std::accumulate(sinks_.begin(), sinks_.end(), int32_t{0}, add);
  }

  /// Returns the minimal credit of all sinks in O(n).
  int32_t min_credit() const {
    auto e = sinks_.end();
    auto i = std::min_element(sinks_.begin(), e, credit_cmp);
    return i == e ? 0 : i->second.credit;
  }

  local_actor* self() const {
    return self_;
  }

  sinks_map& sinks() {
    return sinks_;
  }

  const sinks_map& sinks() const {
    return sinks_;
  }

  std::deque<T>& buf() {
    return buf_;
  }

  /// Sends up to `dest.credit` stream elements to `dest`, removing them
  /// from `buf_` afterwards.
  void send_batch(const stream_id& sid, sink& dest) {
    if (buf_.empty() || dest.credit == 0)
      return;
    auto chunk = get_chunk(static_cast<size_t>(dest.credit));
    auto csize = static_cast<int32_t>(chunk.size());
    dest.credit -= csize;
    auto cm = make_message(std::move(chunk));
    auto mbp = make_mailbox_element(self_->ctrl(), message_id::make(), {},
                                    make_stream_batch(sid, csize, cm,
                                                      dest.next_batch_id++));
    dest.ptr->enqueue(std::move(mbp), self_->context());
  }

  /// Sends `min_credit()` stream elements to each sink, removing them
  /// from `buf_` afterwards.
  void broadcast_batch() {
    // wrap chunk into a message
    auto chunk = get_chunk(static_cast<size_t>(min_credit()));
    if (chunk.empty())
      return;
    auto csize = static_cast<int32_t>(chunk.size());
    auto cm = make_message(std::move(chunk));
    for (auto& kvp : sinks_) {
      auto& snk = kvp.second;
      auto& dest = snk.ptr;
      auto mbp = make_mailbox_element(self_->ctrl(), message_id::make(), {},
                                      make_stream_batch(kvp.first, csize, cm,
                                                        snk.next_batch_id++));
      dest->enqueue(std::move(mbp), self_->context());
      snk.credit -= csize;
    }
  }

  void abort(strong_actor_ptr& cause, const error& reason) {
    for (auto& x : sinks_) {
      if (x.second.ptr != cause) {
        x.second.ptr->enqueue(
          make_mailbox_element(self_->ctrl(), message_id::make(), {},
                               make_stream_abort(x.first, reason)),
          self_->context());
      }
    }
  }

private:
  std::vector<T> get_chunk(size_t max_chunk_size) {
    auto x = std::min(max_chunk_size, buf_.size());
    std::vector<T> chunk;
    chunk.reserve(x);
    if (x < buf_.size()) {
      auto first = buf_.begin();
      auto last = first + x;
      std::move(first, last, std::back_inserter(chunk));
      buf_.erase(first, last);
    } else {
      std::move(buf_.begin(), buf_.end(), std::back_inserter(chunk));
      buf_.clear();
    }
    return std::move(chunk);
  }

  local_actor* self_;
  std::deque<T> buf_;
  sinks_map sinks_;
};

template <class T>
class upstream {
public:
  using sources_map = std::unordered_map<stream_id, source>;

  upstream(local_actor* self) : self_(self) {
    // nop
  }

  sources_map& sources() {
    return sources_;
  }

  void abort(strong_actor_ptr& cause, const error& reason) {
    for (auto& x : sources_) {
      if (x.second.ptr != cause) {
        x.second.ptr->enqueue(
          make_mailbox_element(self_->ctrl(), message_id::make(), {},
                               make_stream_abort(x.first, reason)),
          self_->context());
      }
    }
  }

private:
  local_actor* self_;
  int32_t unassigned_credit_;
  int32_t max_credit_;
  int32_t low_watermark_;
  std::unordered_map<stream_id, source> sources_;
};

/// Mixin for streams with any number of downstreams.
template <class Base, class Subtype>
class any_downstreams : public Base {
public:
  error add_downstream(stream_id& sid, strong_actor_ptr& ptr) final {
    printf("%d -- sid: %s, ptr: %s\n", __LINE__, deep_to_string(sid).c_str(), deep_to_string(ptr).c_str());
    if (!ptr)
      return sec::invalid_argument;
    auto& ds = dptr()->out;
    auto i = ds.sinks().find(sid);
    if (i == ds.sinks().end()) {
      ds.sinks().emplace(sid, sink{ptr});
      return none;
    }
    return sec::downstream_already_exists;
  }

  error trigger_send(strong_actor_ptr&) final {
    printf("%d\n", __LINE__);
    auto& ds = dptr()->out;
    if (!ds.buf().empty())
      dptr()->policy.send_batches(ds);
    return none;
  }

private:
  Subtype* dptr() {
    return static_cast<Subtype*>(this);
  }
};

/// Mixin for streams with any number of upstream actors.
template <class Base, class Subtype>
class any_upstreams : public Base {
public:
  error add_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::cannot_add_upstream;
  }

  error close_upstream(stream_id&, strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot add upstream to a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

  error trigger_demand(strong_actor_ptr&) final {
    CAF_LOG_ERROR("Cannot trigger demand in a stream marked as no-upstreams");
    return sec::invalid_upstream;
  }

private:
  Subtype* dptr() {
    return static_cast<Subtype*>(this);
  }
};

namespace policy {

/// A stream source policy striving to use all available credit
/// without considering ordering or observed downstream behavior.
class bruteforce {
public:
  template <class T>
  void send_batches(downstream<T>& out) {
    printf("%d\n", __LINE__);
    for (auto& kvp : out.sinks())
      out.send_batch(kvp.first, kvp.second);
  }

  template <class T>
  size_t desired_buffer_size(downstream<T>& out) {
    return out.total_credit();
  }
};

} // namespace policy

/// Manages a single stream with no upstreams and any number of downstreams.
template <class Producer, class Policy>
class stream_source : public extend<stream_handler,
                                    stream_source<Producer, Policy>>::template
                             with<any_downstreams, no_upstreams> {
public:
  using output_type = typename Producer::output_type;

  template <class... Ts>
  stream_source(local_actor* self, Policy policy, Ts&&... xs)
      : processor(std::forward<Ts>(xs)...),
        out(self),
        policy(std::move(policy)) {
    // nop
  }

  error add_credit(stream_id& sid, strong_actor_ptr& ptr, int32_t value) final {
    auto& ds = out;
    printf("%d -- sid: %s, sinks: %s\n", __LINE__, deep_to_string(sid).c_str(), deep_to_string(ds.sinks()).c_str());
    auto& processor_ref = processor;
    auto& policy_ref = policy;
    auto i = ds.sinks().find(sid);
    if (i != ds.sinks().end()) {
      i->second.credit += value;
      if (!processor_ref.closed()) {
        printf("%d\n", __LINE__);
        auto current_size = ds.buf().size();
        auto size_hint = policy_ref.desired_buffer_size(ds);
        printf("%d -- %d %d\n", __LINE__, (int) current_size, (int) size_hint);
        if (current_size < size_hint)
          processor_ref(ds, size_hint - current_size);
      }
      return this->trigger_send(ptr);
    }
    return sec::invalid_downstream;
  }

  message make_output_token(const stream_id& x) const override {
    return make_message(stream<output_type>{x});
  }

  void abort(strong_actor_ptr& cause, const error& reason) override {
    out.abort(cause, reason);
  }

  Producer processor;
  downstream<output_type> out;
  Policy policy;
};

/// Manages a single stream with no downstreams and any number of upstreams.
template <class Consumer, class Policy>
class stream_sink : public extend<stream_handler,
                                  stream_sink<Consumer, Policy>>::template
                           with<any_upstreams, no_downstreams> {
public:
  using input_type = typename Consumer::input_type;

  template <class... Ts>
  stream_sink(local_actor* self, Policy policy,
              strong_actor_ptr&& original_sender,
              std::vector<strong_actor_ptr>&& next_stages, Ts&&... xs)
      : self_(self),
        policy_(std::move(policy)),
        original_sender_(std::move(original_sender)),
        next_stages_(std::move(next_stages)),
        consumer_(std::forward<Ts>(xs)...),
        in_(self) {
    // nop
  }

  error handle_data(stream_id&, strong_actor_ptr&, message& xs) final {
    /*
    auto srcs = in_.sources();
    auto i = srcs.find(sid);
    if (i != srcs.end()) {
      
    }
    */
    consumer_(xs);
    return none;
  }

  void abort(strong_actor_ptr& cause, const error& reason) override {
    in_.abort(cause, reason);
  }

  Consumer& consumer() {
    return consumer_;
  }

private:
  local_actor* self_;
  Policy policy_;
  strong_actor_ptr original_sender_;
  std::vector<strong_actor_ptr> next_stages_;
  Consumer consumer_;
  upstream<input_type> in_;
};

/// Manages a single stream with any number of up- and downstream actors.
template <class Processor, class Policy>
class stream_stage : public extend<stream_handler,
                                   stream_stage<Processor, Policy>>::template
                            with<any_upstreams, any_downstreams> {
public:
  /// Type of values received from upstream actors.
  using input_type = typename Processor::input_type;

  /// Type of values sent to downstream actors.
  using output_type = typename Processor::output_type;

  template <class... Ts>
  stream_stage(local_actor* self, Policy p, Ts&&... xs)
      : processor(std::forward<Ts>(xs)...),
        in(self),
        out(self),
        policy(p) {
    // nop
  }

  error add_credit(stream_id& sid, strong_actor_ptr& ptr, int32_t value) final {
    auto& ds = out;
    printf("%d -- sid: %s, sinks: %s\n", __LINE__, deep_to_string(sid).c_str(), deep_to_string(ds.sinks()).c_str());
    auto i = ds.sinks().find(sid);
    if (i != ds.sinks().end()) {
      i->second.credit += value;
      return this->trigger_send(ptr);
    }
    return sec::invalid_downstream;
  }

  error handle_data(stream_id&, strong_actor_ptr&, message& xs) final {
    processor(out, xs);
    strong_actor_ptr dummy;
    this->trigger_send(dummy);
    return none;
  }

  message make_output_token(const stream_id& x) const override {
    return make_message(stream<output_type>{x});
  }

  void abort(strong_actor_ptr& cause, const error& reason) override {
    in.abort(cause, reason);
    out.abort(cause, reason);
  }

  Processor processor;
  upstream<input_type> in;
  downstream<output_type> out;
  Policy policy;
};

template <class F>
struct stream_source_trait;

template <class State, class T>
struct stream_source_trait<void (State&, downstream<T>&, int32_t)> {
  using output = T;
  using state = State;
};

template <class F>
using stream_source_trait_t = stream_source_trait<typename detail::get_callable_trait<F>::fun_sig>;

template <class F>
struct stream_stage_trait;

template <class State, class In, class Out>
struct stream_stage_trait<void (State&, downstream<Out>&, In)> {
  using state = State;
  using input = In;
  using output = Out;
};

template <class F>
using stream_stage_trait_t = stream_stage_trait<typename detail::get_callable_trait<F>::fun_sig>;

template <class Fun, class Fin>
struct stream_sink_trait;

template <class State, class In, class Out>
struct stream_sink_trait<void (State&, In), Out (State&)> {
  using state = State;
  using input = In;
  using output = Out;
};

template <class Fun, class Fin>
using stream_sink_trait_t =
  stream_sink_trait<typename detail::get_callable_trait<Fun>::fun_sig,
                    typename detail::get_callable_trait<Fin>::fun_sig>;

template <class State, class F, class Pred>
class data_source {
public:
  using output_type = typename stream_source_trait_t<F>::output;

  data_source(F fun, Pred pred) : fun_(std::move(fun)), pred_(std::move(pred)) {
    // nop
  }

  void operator()(downstream<output_type>& out, int32_t num) {
    fun_(state_, out, num);
  }

  bool closed() const {
    return pred_(state_);
  }

  State& state() {
    return state_;
  }

private:
  State state_;
  F fun_;
  Pred pred_;
};

template <class State, class F, class Cleanup>
class data_processor {
public:
  using output_type = typename stream_stage_trait_t<F>::output;

  using input_type = typename stream_stage_trait_t<F>::input;

  data_processor(F fun, Cleanup cleanup)
      : fun_(std::move(fun)),
        cleanup_(std::move(cleanup)) {
    // nop
  }

  error operator()(downstream<output_type>& out, message& msg) {
    using vec_type = std::vector<output_type>;
    if (msg.match_elements<vec_type>()) {
      auto& xs = msg.get_as<vec_type>(0);
      for (auto& x : xs)
        fun_(state_, out, x);
      return none;
    }
    return sec::unexpected_message;
  }

  bool closed() const {
    // TODO: implement me
    return false;
  }

  State& state() {
    return state_;
  }

private:
  State state_;
  F fun_;
  Cleanup cleanup_;
};

template <class State, class F, class Finalize>
class data_sink {
public:
  using trait = stream_sink_trait_t<F, Finalize>;

  using input_type = typename trait::input;

  using output_type = typename trait::output;

  data_sink(F fun, Finalize fin) : fun_(std::move(fun)), fin_(std::move(fin)) {
    // nop
  }

  error operator()(message& msg) {
    using vec_type = std::vector<output_type>;
    if (msg.match_elements<vec_type>()) {
      auto& xs = msg.get_as<vec_type>(0);
      for (auto& x : xs)
        fun_(state_, x);
      return none;
    }
    return sec::unexpected_message;
  }

  void finalize() {
    return fin_(state_);
  }

  State& state() {
    return state_;
  }

private:
  State state_;
  F fun_;
  Finalize fin_;
};

class mock_actor;

struct stream_stage_visitor {
public:
  using result_type = void;

  stream_stage_visitor(mock_actor* self, stream_id& sid)
      : self_(self),
        sid_(sid) {
    // nop
  }
  void operator()(stream_msg::open& x);

  void operator()(stream_msg::ok&);

  void operator()(stream_msg::batch&);

  void operator()(stream_msg::demand&) {
    // TODO
  }

  void operator()(stream_msg::close&) {
    // TODO
  }

  void operator()(stream_msg::abort&);

  void operator()(stream_msg::downstream_failed&) {
    // TODO
  }

  void operator()(stream_msg::upstream_failed&) {
    // TODO
  }


private:
  template <class From, class T>
  void async_send(From&& from, strong_actor_ptr& dest,
                  std::vector<strong_actor_ptr> stages, T&& x) {
    dest->enqueue(make_mailbox_element(std::forward<From>(from),
                                       message_id::make(), std::move(stages),
                                       std::forward<T>(x)),
                  nullptr);
  }

  template <class From, class T>
  void async_send(From&& from, strong_actor_ptr& dest, T&& x) {
    async_send(std::forward<From>(from), dest, {}, std::forward<T>(x));
  }

  mock_actor* self_;
  stream_id& sid_;
};

class mock_actor : public event_based_actor {
public:
  using signatures = none_t;
  using behavior_type = behavior;

  mock_actor(actor_config& cfg) : event_based_actor(cfg) {
    default_.assign(
      [=](stream_msg& x) {
        auto self = this;
        debug(self->current_mailbox_element()->content());
        stream_stage_visitor f{this, x.sid};
        apply_visitor(f, x.content);
      }
    );
    set_default_handler([=](scheduled_actor*, message_view& msg) -> result<message> {
      if (!default_(msg.content())) {
        print_and_drop(this, msg);
      }
      return none;
    });
  }

  template <class Init, class Getter, class ClosedPredicate>
  stream<typename stream_source_trait_t<Getter>::output>
  add_source(Init init, Getter getter, ClosedPredicate pred) {
    CAF_ASSERT(current_mailbox_element() != nullptr);
    auto self = this;
    debug("mock_actor: add source");
    using type = typename stream_source_trait_t<Getter>::output;
    using state_type = typename stream_source_trait_t<Getter>::state;
    static_assert(std::is_same<
                    void (state_type&),
                    typename detail::get_callable_trait<Init>::fun_sig
                  >::value,
                  "Expected signature `void (State&)` for init function");
    static_assert(std::is_same<
                    bool (const state_type&),
                    typename detail::get_callable_trait<ClosedPredicate>::fun_sig
                  >::value,
                  "Expected signature `bool (const State&)` for "
                  "closed_predicate function");
    if (current_mailbox_element()->stages.empty()) {
      CAF_LOG_ERROR("cannot create a stream data source without downstream");
      auto rp = make_response_promise();
      rp.deliver(sec::no_downstream_stages_defined);
      return stream_id{nullptr, 0};
    }
    stream_id sid{ctrl(),
                  new_request_id(message_priority::normal).integer_value()};
    fwd_stream_handshake<type>(sid);
    using source_type = data_source<state_type, Getter, ClosedPredicate>;
    using impl = stream_source<source_type, policy::bruteforce>;
    policy::bruteforce p;
    auto ptr = make_counted<impl>(this, p, std::move(getter), std::move(pred));
    init(ptr->processor.state());
    streams_.emplace(std::move(sid), std::move(ptr));
    return sid;
  }

  template <class In, class Init, class Fun, class Cleanup>
  stream<typename stream_stage_trait_t<Fun>::output>
  add_stage(stream<In>& in, Init init, Fun fun, Cleanup cleanup) {
    CAF_ASSERT(current_mailbox_element() != nullptr);
    auto self = this;
    debug("mock_actor: add stage; sid = ", in.id());
    using output_type = typename stream_stage_trait_t<Fun>::output;
    using state_type = typename stream_stage_trait_t<Fun>::state;
    static_assert(std::is_same<
                    void (state_type&),
                    typename detail::get_callable_trait<Init>::fun_sig
                  >::value,
                  "Expected signature `void (State&)` for init function");
    if (current_mailbox_element()->stages.empty()) {
      CAF_LOG_ERROR("cannot create a stream data source without downstream");
      return stream_id{nullptr, 0};
    }
    auto sid = in.id();
    fwd_stream_handshake<output_type>(in.id());
    using processor_type = data_processor<state_type, Fun, Cleanup>;
    using impl = stream_stage<processor_type, policy::bruteforce>;
    policy::bruteforce p;
    auto ptr = make_counted<impl>(this, p, std::move(fun), std::move(cleanup));
    init(ptr->processor.state());
    streams_.emplace(sid, std::move(ptr));
    return std::move(sid);
  }

  template <class In, class Init, class Fun, class Finalize>
  result<typename stream_sink_trait_t<Fun, Finalize>::output>
  add_sink(stream<In>& in, Init init, Fun fun, Finalize finalize) {
    CAF_ASSERT(current_mailbox_element() != nullptr);
    delegated<typename stream_sink_trait_t<Fun, Finalize>::output> dummy_res;
    auto self = this;
    debug("mock_actor: add sink; sid = ", in.id());
    using output_type = typename stream_sink_trait_t<Fun, Finalize>::output;
    using state_type = typename stream_sink_trait_t<Fun, Finalize>::state;
    static_assert(std::is_same<
                    void (state_type&),
                    typename detail::get_callable_trait<Init>::fun_sig
                  >::value,
                  "Expected signature `void (State&)` for init function");
    static_assert(std::is_same<
                    void (state_type&, In),
                    typename detail::get_callable_trait<Fun>::fun_sig
                  >::value,
                  "Expected signature `void (State&, Input)` "
                  "for consume function");
    auto mptr = current_mailbox_element();
    if (!mptr) {
      CAF_LOG_ERROR("add_sink called outside of a message handler");
      debug("add_sink called outside of a message handler");
      return dummy_res;
    }
    using sink_type = data_sink<state_type, Fun, Finalize>;
    using impl = stream_sink<sink_type, policy::bruteforce>;
    policy::bruteforce p;
    auto ptr = make_counted<impl>(this, p, std::move(mptr->sender),
                                  std::move(mptr->stages), std::move(fun),
                                  std::move(finalize));
    init(ptr->consumer().state());
    streams_.emplace(in.id(), std::move(ptr));

    /*
    // store remaining path for the result message
    auto& stages = mptr->stages;
    auto& from = mptr->sender;
    next->enqueue(make_mailbox_element(ctrl(), message_id::make(),
                                       std::move(stages),
                                       make_message(make_stream_open(token))),
                  context());
    using processor_type = data_processor<state_type, Fun, Cleanup>;
    using impl = stream_sink<processor_type, policy::bruteforce>;
    policy::bruteforce p;
    auto ptr = make_counted<impl>(this, p, std::move(fun), std::move(cleanup));
    init(ptr->processor.state());
    streams_.emplace(sid, std::move(ptr));
    */
    return dummy_res;
  }

  message_handler default_;
  std::unordered_map<stream_id, stream_handler_ptr> streams_;

private:
  /// Forwards stream handshake to the next stage.
  template <class T>
  void fwd_stream_handshake(const stream_id& sid) {
    auto mptr = current_mailbox_element();
    auto& stages = mptr->stages;
    CAF_ASSERT(!stages.empty());
    CAF_ASSERT(stages.back() != nullptr);
    auto next = std::move(stages.back());
    stages.pop_back();
    stream<T> token{sid};
    next->enqueue(make_mailbox_element(mptr->sender, mptr->mid,
                                       std::move(stages),
                                       make_stream_open(token, ctrl())),
                  context());
  }
};

void stream_stage_visitor::operator()(stream_msg::open& x) {
  CAF_LOG_TRACE(CAF_ARG(x));
  CAF_ASSERT(self_->current_mailbox_element() != nullptr);
  auto self = self_;
  debug(x);
  auto& predecessor = x.prev_stage;
  auto fail = [&](sec reason) {
    auto rp = self_->make_response_promise();
    rp.deliver(reason);
    async_send(self_->ctrl(), predecessor, make_stream_abort(sid_, reason));
  };
  //auto predecessor = self_->current_sender();
  if (!predecessor) {
    debug(x);
    CAF_LOG_WARNING("received stream_msg::open with empty prev_stage");
    fail(sec::invalid_upstream);
    return;
  }
  auto& bs = self_->bhvr_stack();
  if (bs.empty()) {
    CAF_LOG_WARNING("cannot open stream in actor without behavior");
    debug(x);
    fail(sec::stream_init_failed);
    return;
  }
  auto bhvr = self_->bhvr_stack().back();
  auto res = bhvr(x.token);
  if (!res) {
    CAF_LOG_WARNING("stream handshake failed: actor did not respond to token:"
                    << CAF_ARG(x.token));
    fail(sec::stream_init_failed);
    return;
  }
  debug(self->streams_);
  auto e = self_->streams_.end();
  auto i = self_->streams_.find(sid_);
  if (i == e) {
    debug("i == e");
    CAF_LOG_WARNING("stream handshake failed: actor did not provide a stream "
                    "handler after receiving token:"
                    << CAF_ARG(x.token));
    fail(sec::stream_init_failed);
  } else {
    // check whether we are a stage in a longer pipeline and send more
    // stream_open messages if required
    auto& handler = i->second;
    debug("handler->is_sink(): ", handler->is_sink());
    async_send(self_->ctrl(), predecessor,
               make_message(make_stream_ok(std::move(sid_), 5)));
  }
}

void stream_stage_visitor::operator()(stream_msg::abort& x) {
  CAF_LOG_TRACE(CAF_ARG(x));
  auto self = self_;
  debug(x);
  auto e = self_->streams_.end();
  auto i = self->streams_.find(sid_);
  if (i == e) {
    CAF_LOG_DEBUG("received stream_msg::abort for unknown stream");
  } else {
    i->second->abort(self_->current_sender(), x.reason);
    self_->streams_.erase(i);
  }
}

void stream_stage_visitor::operator()(stream_msg::ok& x) {
  CAF_LOG_TRACE(CAF_ARG(x));
  auto self = self_;
  debug(x);
  auto e = self_->streams_.end();
  auto i = self_->streams_.find(sid_);
  if (i == e) {
    CAF_LOG_DEBUG("received stream_msg::ok for unknown stream");
  } else {
    auto err = i->second->add_downstream(sid_, self_->current_sender());
    if (!err)
      err = i->second->add_credit(sid_, self_->current_sender(), x.initial_demand);
    if (err)
      i->second->abort(self_->current_sender(), err);
  }
}

void stream_stage_visitor::operator()(stream_msg::batch& x) {
  CAF_LOG_TRACE(CAF_ARG(x));
  auto self = self_;
  debug(x);
  auto e = self_->streams_.end();
  auto i = self_->streams_.find(sid_);
  if (i == e) {
    CAF_LOG_DEBUG("received stream_msg::batch for unknown stream");
  } else {
    auto err = i->second->handle_data(sid_, self_->current_sender(), x.xs);
    if (err) {
    printf("err: %s\n", to_string(err).c_str());

      i->second->abort(self_->current_sender(), err);
    }
  }
}

behavior file_reader_(mock_actor* self) {
  using vec = std::vector<int>;
  return {
    [=](const std::string& path) -> stream<int> {
      debug("file_reader ; path = ", path);
      return self->add_source(
        // initialize state
        [&](vec& xs) {
          debug("file_reader: init stream");
          xs = vec{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
        },
        // get next element
        [=](vec& xs, downstream<int>& out, int32_t num) {
          auto n = std::min(num, static_cast<int32_t>(xs.size()));
          debug("file_reader: get ", n, " elements");
          for (int32_t i = 0; i < n; ++i)
            out.push(xs[i]);
          xs.erase(xs.begin(), xs.begin() + static_cast<size_t>(n));
        },
        // check whether we reached the end
        [=](const vec& xs) {
          if (xs.empty())
            debug("file_reader: reached the end");
          return xs.empty();
        }
      );
    }
  };
}

behavior filter_(mock_actor* self) {
  return {
    [=](stream<int>& x) -> stream<int> {
      debug("filter_ ; x = ", x);
      return self->add_stage(
        // input stream
        x,
        // initialize state
        [=](unit_t&) {
          debug("");
          // nop
        },
        // processing step
        [=](unit_t&, downstream<int>& out, int x) {
          debug("filter_ ; x = ", x);
          if (x & 0x01)
            out.push(x);
        },
        // cleanup
        [=](unit_t&) {
          debug("");
          // nop
        }
      );
    }
  };
}

behavior broken_filter_(mock_actor* self) {
  return {
    [=](stream<int>& x) -> stream<int> {
      debug("broken_filter_ ; x = ", x);
      return x;
    }
  };
}

behavior sum_up_(mock_actor* self) {
  return {
    [=](stream<int>& x) {
      debug("sum_up_; x = ", x);
      return self->add_sink(
        // input stream
        x,
        // initialize state
        [](int& x) {
          x = 0;
        },
        // processing step
        [](int& x, int y) {
          x += y;
        },
        // cleanup and produce result message
        [](int& x) -> int {
          return x;
        }
      );
    }
  };
}

struct config : actor_system_config {
  config() {
    scheduler_policy = atom("testing");
  }
};

struct fixture {
  config cfg;
  actor_system sys;
  scoped_actor self;
  scheduler::test_coordinator& sched;

  fixture()
      : sys(cfg),
        self(sys),
        sched(dynamic_cast<scheduler::test_coordinator&>(sys.scheduler())) {
    sys.await_actors_before_shutdown(false);
  }

  template <class T = int>
  expected<T> fetch_result() {
    expected<T> result = error{};
    self->receive(
      [&](T& x) {
        result = std::move(x);
      },
      [&](error& x) {
        result = std::move(x);
      }
    );
    return result;
  }

  template <class T = stream_msg>
  const T& peek() {
    auto ptr = sched.next_job<local_actor>().mailbox().peek();
    CAF_REQUIRE(ptr != nullptr);
    CAF_MESSAGE(to_string(ptr->content()));
    CAF_REQUIRE(ptr->content().match_elements<T>());
    return ptr->content().get_as<T>(0);
  }

  template <class T>
  const T& unbox(const stream_msg& x) {
    CAF_REQUIRE(holds_alternative<T>(x.content));
    return get<T>(x.content);
  }

  template <class T>
  const T& peek_unboxed() {
    return unbox<T>(peek());
  }

  mock_actor& deref(actor& hdl) {
    auto ptr = actor_cast<abstract_actor*>(hdl);
    CAF_REQUIRE(ptr != nullptr);
    return dynamic_cast<mock_actor&>(*ptr);
  }
};

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(streaming_tests, fixture)

CAF_TEST(no_downstream) {
  CAF_MESSAGE("opening streams must fail if no downstream stage exists");
  auto source = sys.spawn(file_reader_);
  self->send(source, "test.txt");
  sched.run();
  CAF_CHECK_EQUAL(fetch_result(), sec::no_downstream_stages_defined);
  CAF_CHECK(deref(source).streams_.empty());
}

CAF_TEST(broken_pipeline) {
  CAF_MESSAGE("streams must abort if a stage fails to initialize its state");
  using iface_src = typed_actor<replies_to<std::string>::with<stream<int>>>;
  auto source = sys.spawn(file_reader_);
  auto stage = sys.spawn(broken_filter_);
  auto pipeline = stage * source;
  sched.run();
  // self --("test.txt")--> source
  self->send(pipeline, "test.txt");
  CAF_REQUIRE(sched.prioritize(source));
  CAF_REQUIRE_EQUAL(peek<std::string>(), "test.txt");
  sched.run_once();
  // source --(stream_msg::open)--> stage
  CAF_REQUIRE(sched.prioritize(stage));
  auto& open_msg = peek_unboxed<stream_msg::open>();
  CAF_CHECK_EQUAL(open_msg.prev_stage, source);
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(deref(stage).streams_.empty());
  // stage --(stream_msg::abort)--> source
  auto& abort_msg = peek_unboxed<stream_msg::abort>();
  CAF_CHECK_EQUAL(sys.render(abort_msg.reason),
                  sys.render(sec::stream_init_failed));
  sched.run_once();
  CAF_CHECK(deref(source).streams_.empty());
  CAF_CHECK(deref(stage).streams_.empty());
  CAF_CHECK_EQUAL(fetch_result(), sec::stream_init_failed);
}

CAF_TEST(incomplete_pipeline) {
  CAF_MESSAGE("streams must abort if not reaching a sink");
  auto source = sys.spawn(file_reader_);
  auto stage = sys.spawn(filter_);
  auto pipeline = stage * source;
  sched.run();
  // self --("test.txt")--> source
  self->send(pipeline, "test.txt");
  CAF_REQUIRE(sched.prioritize(source));
  CAF_REQUIRE_EQUAL(peek<std::string>(), "test.txt");
  sched.run_once();
  // source --(stream_msg::open)--> stage
  CAF_REQUIRE(sched.prioritize(stage));
  auto& open_msg = peek_unboxed<stream_msg::open>();
  CAF_CHECK_EQUAL(open_msg.prev_stage, source);
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(deref(stage).streams_.empty());
  // stage --(stream_msg::abort)--> source
  CAF_REQUIRE(sched.prioritize(source));
  auto& abort_msg = peek_unboxed<stream_msg::abort>();
  CAF_CHECK_EQUAL(sys.render(abort_msg.reason),
                  sys.render(sec::stream_init_failed));
  sched.run_once();
  CAF_CHECK(deref(source).streams_.empty());
  CAF_CHECK(deref(stage).streams_.empty());
  CAF_CHECK_EQUAL(fetch_result(), sec::stream_init_failed);
}

CAF_TEST(full_pipeline) {
  CAF_MESSAGE("check fully initialized pipeline");
  auto source = sys.spawn(file_reader_);
  auto stage = sys.spawn(filter_);
  auto sink = sys.spawn(sum_up_);
  auto pipeline = sink * stage * source;
  // run initialization code
  sched.run();
  // self --("test.txt")--> source
  self->send(pipeline, "test.txt");
  CAF_REQUIRE(sched.prioritize(source));
  CAF_REQUIRE_EQUAL(peek<std::string>(), "test.txt");
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(deref(stage).streams_.empty());
  CAF_CHECK(deref(sink).streams_.empty());
  // source --(stream_msg::open)--> stage
  CAF_REQUIRE(sched.prioritize(stage));
  auto& open_msg = peek_unboxed<stream_msg::open>();
  CAF_CHECK_EQUAL(open_msg.prev_stage, source);
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(!deref(stage).streams_.empty());
  CAF_CHECK(deref(sink).streams_.empty());
  // stage --(stream_msg::open)--> sink
  CAF_REQUIRE(sched.prioritize(sink));
  auto& open_msg2 = peek_unboxed<stream_msg::open>();
  CAF_CHECK_EQUAL(open_msg2.prev_stage, stage);
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(!deref(stage).streams_.empty());
  CAF_CHECK(!deref(sink).streams_.empty());
  // sink --(stream_msg::ok)--> stage
  CAF_REQUIRE(sched.prioritize(stage));
  auto& ok_msg = peek_unboxed<stream_msg::ok>();
  CAF_CHECK_EQUAL(ok_msg.initial_demand, 5);
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(!deref(stage).streams_.empty());
  CAF_CHECK(!deref(sink).streams_.empty());
  // stage --(stream_msg::ok)--> source 
  CAF_REQUIRE(sched.prioritize(source));
  auto& ok_msg2 = peek_unboxed<stream_msg::ok>();
  CAF_CHECK_EQUAL(ok_msg2.initial_demand, 5);
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(!deref(stage).streams_.empty());
  CAF_CHECK(!deref(sink).streams_.empty());
  // source --(stream_msg::batch)--> stage
  CAF_REQUIRE(sched.prioritize(stage));
  auto& batch = peek_unboxed<stream_msg::batch>();
  CAF_CHECK_EQUAL(batch.xs_size, 5);
  CAF_CHECK_EQUAL(to_string(batch.xs), "([1, 2, 3, 4, 5])");
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(!deref(stage).streams_.empty());
  CAF_CHECK(!deref(sink).streams_.empty());
  // stage --(stream_msg::batch)--> sink
  CAF_REQUIRE(sched.prioritize(sink));
  auto& batch2 = peek_unboxed<stream_msg::batch>();
  CAF_CHECK_EQUAL(batch2.xs_size, 3);
  CAF_CHECK_EQUAL(to_string(batch2.xs), "([1, 3, 5])");
  sched.run_once();
  CAF_CHECK(!deref(source).streams_.empty());
  CAF_CHECK(!deref(stage).streams_.empty());
  CAF_CHECK(!deref(sink).streams_.empty());
}

CAF_TEST_FIXTURE_SCOPE_END()
