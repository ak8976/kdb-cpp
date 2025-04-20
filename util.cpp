#include <mutex>
extern "C" {
#include "k.h"
}
using namespace std;
static mutex m;
extern "C" K insert_safe(K ref, K data) {
  if (ref->t == XT) {
    K tcols = kK(ref->k)[1];
    K rcols = kK(data->k)[1];
    m.lock();
    for (size_t i = 0; i < tcols->n; ++i) {
      jv(&kK(tcols)[i], kK(rcols)[i]);
    }
    m.unlock();
  }
  return (K)0;
}
extern "C" K add_safe(K x, K y) {
  if (x->t == -KJ) {
    if (y->t == -KJ) {
      m.lock();
      x->j += y->j;
      m.unlock();
    }
  } else if (x->t == KJ) {
    if (y->t == -KJ) {
      m.lock();
      for (size_t i = 0; i < x->n; ++i) {
        kJ(x)[i] += y->j;
      }
      m.unlock();
    } else if (y->t == KJ && x->n == y->n) {
      m.lock();
      for (size_t i = 0; i < x->n; ++i) {
        kJ(x)[i] += kJ(y)[i];
      }
      m.unlock();
    }
  }
  return (K)0;
}
