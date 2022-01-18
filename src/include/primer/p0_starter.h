//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <memory>
#include <utility>
#include "common/logger.h"
namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) : rows(r), cols(c), linear(nullptr) { linear = new T[r * c]; }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c), data_(nullptr) {
    data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      data_[i] = new T[c];
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    // 一行元素所占的内存大小
    int size = sizeof(T) * this->cols;
    for (int i = 0; i < this->rows; i++) {
      memcpy(data_[i], arr + i * this->cols, size);
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() final {
    if (data_ != nullptr) {
      for (int i = 0; i < this->rows; i++) {
        delete[] data_[i];
      }
      delete[] data_;
    }
  }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    auto res = std::unique_ptr<RowMatrix<T>>(nullptr);
    if (mat1 == nullptr || mat2 == nullptr) {
      LOG_DEBUG("AddMatrices :mat1 or mat2 is nullptr\n");
      return res;
    }
    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      LOG_DEBUG("AddMatrices: dimensions mismatch\n");
      return res;
    }

    int rows = mat1->GetRows();
    int cols = mat1->GetColumns();
    res = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(rows, cols));
    T temp_res;
    T num1;
    T num2;
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        num1 = mat1->GetElem(i, j);
        num2 = mat2->GetElem(i, j);
        temp_res = num1 + num2;
        res->SetElem(i, j, temp_res);
      }
    }
    return res;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    auto res = std::unique_ptr<RowMatrix<T>>(nullptr);
    if (mat1 == nullptr || mat2 == nullptr) {
      LOG_DEBUG("MultiplyMatrices :mat1 or mat2 is nullptr\n");
      return res;
    }
    if (mat1->GetColumns() != mat2->GetRows()) {
      LOG_DEBUG("AddMatrices: dimensions mismatch\n");
      return res;
    }

    int rows = mat1->GetRows();
    int cols = mat2->GetColumns();
    int size = mat1->GetColumns();
    res = std::unique_ptr<RowMatrix<T>>(new RowMatrix<T>(rows, cols));
    T temp_res;
    T num1;
    T num2;

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        T sum = 0;
        for (int k = 0; k < size; k++) {
          num1 = mat1->GetElem(i, k);
          num2 = mat2->GetElem(k, j);
          temp_res = num1 * num2;
          sum = sum + temp_res;
        }
        res->SetElem(i, j, sum);
      }
    }
    return res;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    std::unique_ptr<RowMatrix<T>> res = std::unique_ptr<RowMatrix<T>>(nullptr);
    std::unique_ptr<RowMatrix<T>> temp = MultiplyMatrices(std::move(matA), std::move(matB));
    if (temp == nullptr) {
      return res;
    }
    res = AddMatrices(std::move(temp), std::move(matC));
    return res;
  }
};
}  // namespace bustub
