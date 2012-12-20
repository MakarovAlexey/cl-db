;;;; cl-db.lisp

(in-package #:cl-db)

;;; "cl-db" goes here. Hacks and glory await!

(defclass persistent-class (standard-class)
  ())

(defmethod validate-superclass ((class persistent-class)
				(superclass standard-class))
  t)

(defclass persistent-store () ())

(defgeneric open-store (store))

(defgeneric close-store (store))

(defgeneric begin-transaction (store))

(defgeneric commit (transaction))

;; slots and associated relationships
;; inheritance relationships
;; transactions
