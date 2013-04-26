;;;; package.lisp

(defpackage #:cl-db
  (:use #:closer-common-lisp)
  (:export :table :column :foreign-key
	   :map-class
	   :map-slot
	   :value
	   :one-to-many
	   :many-to-one
	   :make-persistence-unit))

