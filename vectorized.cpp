#include <iostream>
#include <xsimd/xsimd.hpp>
#include "k.h"
#if defined(__AVX512F__)
constexpr int available_registers = 24;
#elif defined(__AVX2__)
constexpr int available_registers = 12;
#else
constexpr int available_registers = 8;
#endif
template <typename T> inline K add_vector(const K& x, const K& y) {
  T* xptr = reinterpret_cast<T*>(kG(x));
  T* yptr = reinterpret_cast<T*>(kG(y));
  constexpr size_t batch_size = xsimd::batch<T>::size;
  size_t size = x->n;
  size_t vec_size = size - size % batch_size;
  K result = ktn(x->t, size);
  if (size == 0) return result;
  T* __restrict rptr = reinterpret_cast<T*>(kG(result));
  constexpr size_t num_reg_per_iter = 3; // 3 registers needed for x,y,r
  constexpr size_t unroll_factor = available_registers / num_reg_per_iter;
  constexpr size_t unrolled_batch = batch_size * unroll_factor;
  size_t i = 0;
  // compiler will unloop to <unroll_factor> blocks for parallel compute
  for (; i + unrolled_batch < vec_size; i += unrolled_batch) {
    for (size_t j = 0; j < unroll_factor; ++j) {
      __builtin_prefetch(xptr + i + (j + 1) * batch_size, 0, 0);
      __builtin_prefetch(yptr + i + (j + 1) * batch_size, 0, 0);
      xsimd::batch<T> x_batch =
          xsimd::batch<T>::load_unaligned(xptr + i + j * batch_size);
      xsimd::batch<T> y_batch =
          xsimd::batch<T>::load_unaligned(yptr + i + j * batch_size);
      xsimd::batch<T> r_batch = x_batch + y_batch;
      r_batch.store_unaligned(rptr + i + j * batch_size);
    }
  }
  for (; i < vec_size; i += batch_size) {
    xsimd::batch<T> x_batch = xsimd::batch<T>::load_unaligned(xptr + i);
    xsimd::batch<T> y_batch = xsimd::batch<T>::load_unaligned(yptr + i);
    xsimd::batch<T> r_batch = x_batch + y_batch;
    r_batch.store_unaligned(rptr + i);
  }
  for (; i < size; ++i) {
    rptr[i] = xptr[i] + yptr[i];
  }
  return result;
}

template <typename T1, typename T2> K weighted_avg(const K& x, const K& y) {
  T1* xptr = reinterpret_cast<T1*>(kG(x));
  T2* yptr = reinterpret_cast<T2*>(kG(y));
  constexpr size_t batch_size =
      std::min(xsimd::batch<T1>::size, xsimd::batch<T2>::size);
  size_t size = x->n;
  size_t vec_size = size - size % batch_size;
  constexpr size_t num_reg_per_iter = 2; // 2 registers needed for x,y
  constexpr size_t unroll_factor = available_registers / num_reg_per_iter;
  constexpr size_t unrolled_batch = batch_size * unroll_factor;
  size_t i = 0;
  xsimd::batch<F> ws_acc(0);
  xsimd::batch<F> tw_acc(0);
  // compiler will unloop to <unroll_factor> blocks for parallel compute
  for (; i + unrolled_batch < vec_size; i += unrolled_batch) {
    for (size_t j = 0; j < unroll_factor; ++j) {
      __builtin_prefetch(xptr + i + (j + 1) * batch_size, 0, 0);
      __builtin_prefetch(yptr + i + (j + 1) * batch_size, 0, 0);
      xsimd::batch<F> x_batch = xsimd::batch_cast<F>(
          xsimd::batch<T1>::load_unaligned(xptr + i + j * batch_size));
      xsimd::batch<F> y_batch = xsimd::batch_cast<F>(
          xsimd::batch<T2>::load_unaligned(yptr + i + j * batch_size));
      ws_acc += x_batch * y_batch;
      tw_acc += x_batch;
    }
  }
  for (; i < vec_size; i += batch_size) {
    xsimd::batch<F> x_batch =
        xsimd::batch_cast<F>(xsimd::batch<T1>::load_unaligned(xptr + i));
    xsimd::batch<F> y_batch =
        xsimd::batch_cast<F>(xsimd::batch<T2>::load_unaligned(yptr + i));
    ws_acc += x_batch * y_batch;
    tw_acc += x_batch;
  }
  F weighted_sum = xsimd::reduce_add(ws_acc);
  F total_weights = xsimd::reduce_add(tw_acc);
  for (; i < size; ++i) {
    weighted_sum += xptr[i] * yptr[i];
    total_weights += xptr[i];
  }
  K result = ka(-KF);
  result->f = weighted_sum / total_weights;
  return result;
}

extern "C" K vec_add(K x, K y) {
  if (x->t != y->t) return krr((S) "type");
  if (x->n != y->n) return krr((S) "length");
  switch (x->t) {
    case KP:
    case KN:
    case KJ:
      return add_vector<J>(x, y);
    case KI:
    case KM:
    case KD:
    case KU:
    case KV:
    case KT:
      return add_vector<I>(x, y);
    case KF:
      return add_vector<F>(x, y);
    case KE:
      return add_vector<E>(x, y);
    case KH:
      return add_vector<H>(x, y);
  }
  return krr((S) "nyi");
}
extern "C" K vec_wavg(K x, K y) {
  char type_x = x->t;
  char type_y = y->t;
  // just support longs and doubles for now
  if (!(type_x == KF || type_x == KJ)) return krr((S) "type");
  if (!(type_y == KF || type_y == KJ)) return krr((S) "type");
  if (x->n != y->n) return krr((S) "length");
  if (type_x == KJ && type_y == KJ)
    return weighted_avg<J, J>(x, y);
  else if (type_x == KJ and type_y == KF)
    return weighted_avg<J, F>(x, y);
  else if (type_x == KF and type_y == KJ)
    return weighted_avg<F, J>(x, y);
  else if (type_x == KF and type_y == KF)
    return weighted_avg<F, F>(x, y);
  else
    return krr((S) "nyi");
}
