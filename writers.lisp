(in-package #:cl-db)

(defvar *parameters*)

(defun write-expression (stream object &optional a b)
  (declare (ignore a b))
  (if (listp object)
      (destructuring-bind (expression-writer &rest args) object
	(apply expression-writer stream args))
      (progn (push object *parameters*)
	     (format stream "$~a" (length *parameters*)))))

(defun write-select-list (stream &rest args)
  (format stream "SELECT ~{~/cl-db:write-expression/~^,~%~7T~}~%" args))

(defun write-from-clause (stream &rest args)
  (format stream "  FROM ~{~/cl-db:write-expression/~^~1TCROSS JOIN ~}" args))

(defun write-where-clause (stream &rest args)
  (format stream " WHERE ~{~/cl-db:write-expression/~^~%~3TAND ~}" args))

(defun write-group-by-clause (stream &rest args)
  (format stream " GROUP BY ~{~/cl-db:write-expression/~^,~%~10T~}" args))

(defun write-having-clause (stream &rest args)
  (format stream "HAVING ~{~/cl-db:write-expression/~^~%~3TAND ~}" args))

(defun write-order-by-clause (stream &rest args)
  (format stream " ORDER BY ~{~/cl-db:write-expression/~^, ~}" args))

(defun write-limit (stream value)
  (format stream " LIMIT ~a" value))

(defun write-offset (stream value)
  (format stream "OFFSET ~a" value))

(defun write-label (stream expression alias)
  (format stream "~/cl-db:write-expression/ AS ~a" expression alias))

(defun write-column (stream column-name table-alias)
  (format stream "~a.~a" table-alias column-name))

(defun write-table-name (stream table-name)
  (format stream "~a" table-name))

(defun write-inner-join (stream table alias on &rest joins)
  (format stream "~1TINNER JOIN ~a AS ~a~%~4TON ~{~{~/cl-db:write-expression/ = ~/cl-db:write-expression/~%~}~^~3TAND ~}~{~/cl-db:write-expression/~}"
	  table alias on joins))

(defun write-left-join (stream table alias on &rest joins)
  (format stream "~2TLEFT JOIN ~a AS ~a~%~4TON ~{~{~/cl-db:write-expression/ = ~/cl-db:write-expression/~%~}~^~3TAND ~}~{~/cl-db:write-expression/~}"
	  table alias on joins))

(defun write-table-reference (stream table alias &rest joins)
  (format stream "~a AS ~a~%~{~/cl-db:write-expression/~}" table alias joins))

(defun write-and (stream &rest expressions)
  (format stream "~{(~/cl-db:write-expression/)~^ AND ~}" expressions))

(defun write-or (stream &rest expressions)
  (format stream "~{(~/cl-db:write-expression/)~^ OR ~}" expressions))

(defun write-not (stream expression)
  (format stream "NOT (~/cl-db:write-expression/)" expression))

(defun write-less-than (stream lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ < ~/cl-db:write-expression/"
	  lhs-expression rhs-expression))

(defun write-more-than (stream lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ > ~/cl-db:write-expression/"
	  lhs-expression rhs-expression))

(defun write-less-than-or-equal (stream lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ <= ~/cl-db:write-expression/"
	  lhs-expression rhs-expression))

(defun write-more-than-or-equal (stream lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ <= ~/cl-db:write-expression/"
	  lhs-expression rhs-expression))

(defun write-equal (stream lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ = ~/cl-db:write-expression/"
	  lhs-expression rhs-expression))

(defun write-not-equal (stream lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ <> ~/cl-db:write-expression/"
	  lhs-expression rhs-expression))

(defun write-is-null (stream expression)
  (format stream "~/cl-db:write-expression/ IS NULL" expression))

(defun write-is-true (stream expression)
  (format stream "~/cl-db:write-expression/ IS TRUE" expression))

(defun write-is-false (stream expression)
  (format stream "~/cl-db:write-expression/ IS FALSE" expression))

(defun write-between (stream expression lhs-expression rhs-expression)
  (format stream
	  "~/cl-db:write-expression/ BETWEEN ~/cl-db:write-expression/ AND ~/cl-db:write-expression/"
	  expression lhs-expression rhs-expression))

(defun write-like (stream expression pattern)
  (format stream "~/cl-db:write-expression/ LIKE ~/cl-db:write-expression/"
	  expression pattern))

(defun write-like-pattern (stream pattern)
  (format stream "'~a'" pattern))

(defun write-count (stream &rest expressions)
  (format stream "count(~{~/cl-db:write-expression/~^, ~})" expressions))

(defun write-avg (stream expression)
  (format stream "avg(~/cl-db:write-expression/)" expression))

(defun write-every (stream expression)
  (format stream "every(~/cl-db:write-expression/)" expression))

(defun write-max (stream expression)
  (format stream "max(~/cl-db:write-expression/)" expression))

(defun write-min (stream expression)
  (format stream "min(~/cl-db:write-expression/)" expression))

(defun write-sum (stream expression)
  (format stream "sum(~/cl-db:write-expression/)" expression))

(defun write-ascending (stream &rest expressions)
  (format stream "~{~/cl-db:write-expression/ ASC~^, ~}" expressions))

(defun write-descending (stream &rest expressions)
  (format stream "~{~/cl-db:write-expression/ DESC~^, ~}" expressions))

(defun write-sql-string (stream &rest query)
  (let ((*parameters* nil))
    (dolist (sql-clause query (reverse *parameters*))
      (destructuring-bind (function &rest args)
	  sql-clause
	(apply function stream args)))))
