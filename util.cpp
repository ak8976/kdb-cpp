//#include <iostream>
#include <cstring>
#include <mutex>
extern "C" {
#include "k.h"
}
using namespace std;
static mutex m;
extern "C" K insert_safe(K ref, K data) {
  if (ref->t == XT) {
    K col_names = kK(ref->k)[0];
    K col_vals = kK(ref->k)[1];
    if (data->t == XT) {
      K rcol_names = kK(data->k)[0];
      if (col_names->n != rcol_names->n) return krr((S) "mismatch");
      for (size_t i = 0; i < col_names->n; ++i) {
        if (strcmp(kS(col_names)[i], kS(rcol_names)[i]) != 0)
          return krr((S) "mismatch");
      }
      K rcols = kK(data->k)[1];
      m.lock();
      for (size_t i = 0; i < col_vals->n; ++i) {
        jv(&kK(col_vals)[i], kK(rcols)[i]);
      }
      m.unlock();
    } else if (data->t == XD) {
      K rcol_names = kK(data)[0];
      if (col_names->n != rcol_names->n) return krr((S) "mismatch");
      for (size_t i = 0; i < col_names->n; ++i) {
        if (strcmp(kS(col_names)[i], kS(rcol_names)[i]) != 0)
          return krr((S) "mismatch");
      }
      K rcols = kK(data)[1];
      bool mixed = rcols->t == 0;
      m.lock();
      for (size_t i = 0; i < col_vals->n; ++i) {
        K val = kK(rcols)[i];
        if (mixed) {
          switch (val->t) {
            case -KB:
            case -KG:
            case -KC:
              ja(&kK(col_vals)[i], &val->g);
              break;
            case -KH:
              ja(&kK(col_vals)[i], &val->h);
              break;
            case -KI:
            case -KM:
            case -KD:
            case -KU:
            case -KV:
            case -KT:
              ja(&kK(col_vals)[i], &val->i);
              break;
            case -KJ:
            case -KP:
            case -KN:
              ja(&kK(col_vals)[i], &val->j);
              break;
            case -KE:
              ja(&kK(col_vals)[i], &val->e);
              break;
            case -KF:
              ja(&kK(col_vals)[i], &val->f);
              break;
            case -KS:
              ja(&kK(col_vals)[i], &val->s);
              break;
            case KC:
              ja(&kK(col_vals)[i], &val);
              break;
            default:
              m.unlock();
              return krr((S) "unsupported type");
          }
        } else {
          switch (rcols->t) {
            case KB:
            case KG:
            case KC:
              ja(&kK(col_vals)[i], &kC(rcols)[i]);
              break;
            case KH:
              ja(&kK(col_vals)[i], &kH(rcols)[i]);
              break;
            case KI:
            case KM:
            case KD:
            case KU:
            case KV:
            case KT:
              ja(&kK(col_vals)[i], &kI(rcols)[i]);
              break;
            case KJ:
            case KP:
            case KN:
              ja(&kK(col_vals)[i], &kJ(rcols)[i]);
              break;
            case KE:
              ja(&kK(col_vals)[i], &kE(rcols)[i]);
              break;
            case KF:
              ja(&kK(col_vals)[i], &kF(rcols)[i]);
              break;
            case KS:
              ja(&kK(col_vals)[i], &kS(rcols)[i]);
              break;
            default:
              m.unlock();
              return krr((S) "unsupported type");
          }
        }
      }
      m.unlock();
    }
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
