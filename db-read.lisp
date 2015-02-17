(in-package #:cl-db)

(defun binary-operator (operator lhs-expression
			rhs-expression &rest rest-expressions)
  (let ((expression (list* operator
			  (mapcar #'expression-of
				  (list* lhs-expression
					 rhs-expression
					 rest-expressions))))
	(alias (make-alias "op")))
    (make-expression :expression expression
		     :count expression
		     :select-list (list (cons expression alias))
		     :from-clause (reduce #'append
					  (list* rhs-expression
						 lhs-expression
						 rest-expressions)
					  :key #'from-clause-of
					  :initial-value nil)
		     :group-by-clause (reduce #'list*
					      (list* rhs-expression
						     lhs-expression
						     rest-expressions)
					      :key #'group-by-clause-of
					      :initial-value nil)
		     :loader (make-value-loader alias))))

(defun db-and (lhs-expression rhs-expression &rest rest-expressions)
  (apply #'binary-operator #'write-and
	 lhs-expression
	 rhs-expression
	 rest-expressions))

(defun db-or (lhs-expression rhs-expression &rest rest-expressions)
  (apply #'binary-operator #'write-or
	 lhs-expression
	 rhs-expression
	 rest-expressions))

(defun unary-operator (operator argument)
  (let ((expression (list* operator (expression-of argument)))
	(alias (make-alias "op")))
    (make-expression :expression expression
		     :count expression
		     :select-list (list (cons expression alias))
		     :from-clause (from-clause-of argument)
		     :group-by-clause (group-by-clause-of argument)
		     :loader (make-value-loader alias))))

(defun db-not (argument)
  (unary-operator #'write-not argument))

;; Comparison Operators

(defun db-< (lhs-expression rhs-expression)
  (binary-operator #'write-less-than
		   lhs-expression rhs-expression))

(defun db-> (lhs-expression rhs-expression)
  (binary-operator #'write-more-than
		   lhs-expression rhs-expression))

(defun db-<= (lhs-expression rhs-expression)
  (binary-operator #'write-less-than-or-equal
		   lhs-expression rhs-expression))

(defun db->= (lhs-expression rhs-expression)
  (binary-operator #'write-more-than-or-equal
		   lhs-expression rhs-expression))

(defun db-eq (lhs-expression rhs-expression)
  (binary-operator #'write-equal
		   lhs-expression rhs-expression))

(defun db-not-eq (lhs-expression rhs-expression)
  (binary-operator #'write-not-equal
		   lhs-expression rhs-expression))

(defun db-is-null (expression)
  (unary-operator #'write-is-null expression))

(defun db-is-true (expression)
  (unary-operator #'write-is-true expression))

(defun db-is-false (expression)
  (unary-operator #'write-is-false expression))

(defun db-between (argument lhs-expression rhs-expression)
  (let* ((range (db-and lhs-expression rhs-expression))
	 (expression (list* (expression-of argument) :between range))
	 (alias (make-alias "op")))
    (make-expression :expression expression
		     :count expression
		     :select-list (list (cons expression alias))
		     :from-clause (append
				   (from-clause-of argument)
				   (from-clause-of range))
		     :group-by-clause (append
				       (group-by-clause-of argument)
				       (group-by-clause-of range))
		     :loader (make-value-loader alias))))

(defun db-like (expression pattern)
  (binary-operator :like expression pattern))

(defun db-count (expression)
  (let ((count (list* :count (count-expression-of expression)))
	(alias (make-alias "op")))
    (make-expression :expression count
		     :select-list (list (cons count alias))
		     :from-clause (count-from-clause-of expression)
		     :loader (make-value-loader alias))))

(defun aggregate-function (function expression)
  (let ((aggregate (list* function (expression-of expression)))
	(alias (make-alias "op")))
    (make-expression :expression aggregate
		     :select-list (list (cons aggregate alias))
		     :from-clause (from-clause-of expression)
		     :loader (make-value-loader alias))))

(defun db-avg (expression)
  (aggregate-function :avg expression))

(defun db-every (expression)
  (aggregate-function :every expression))

(defun db-max (expression)
  (aggregate-function :max expression))

(defun db-min (expression)
  (aggregate-function :min expression))

(defun db-sum (expression)
  (aggregate-function :sum expression))

;; ORDER BY

(defun ascending (expression)
  #'(lambda (query)
      (list* #'write-ascending
	     (mapcar #'(lambda (expression)
			 (funcall query (alias expression)))
		     (select-list-of expression)))))

(defun descending (expression)
  #'(lambda (query)
      (list* #'write-descending
	     (mapcar #'(lambda (expression)
			 (funcall query (alias expression)))
		     (select-list-of expression)))))

(defun compute-select (select-item &rest select-list)
  (multiple-value-bind (selectors rest-fetched-refernces)
      (when (not (null select-list))
	(apply #'compute-select select-list))
    (values
     (list* select-item selectors)
     (list* (fetch-references-of select-item) rest-fetched-refernces))))

(defun property (mapping reader)
  (funcall (properties-of mapping) reader))
		    
(defun join (selector accessor alias &optional join)
  (let ((joined-selector
	 (funcall (join-references-of selector) accessor)))
    (list* alias joined-selector
	   (when (not (null join))
	     (multiple-value-call #'append
	       (funcall join joined-selector))))))

(defun fetch (references accessor &optional fetch)
  (multiple-value-bind (reference class-loader)
      (funcall references accessor)
    #'(lambda ()
	(values #'(lambda (query loader)
		    (funcall reference query loader fetch))
		class-loader))))

;; (defun fetch-using-subclass (class-name references &rest fetch))

(defun db-read (roots &key join select where order-by having
			offset limit singlep transform fetch
			(mapping-schema *mapping-schema*))
  (declare (ignore transform singlep))
  (let* ((*table-index* 0)
	 (*mapping-schema* mapping-schema)
	 (selectors
	  (if (not (listp roots))
	      (make-join-plan mapping-schema roots)
	      (apply #'make-join-plan mapping-schema roots)))
	 (joined-list
	  (append selectors
		  (when (not (null join))
		    (multiple-value-call #'append
		      (apply join selectors))))))
    (multiple-value-bind (select-list fetch-references)
	(apply #'compute-select
	       (if (not (null select))
		   (multiple-value-list
		    (apply select joined-list))
		   selectors))
      (compute-query select-list
		     (when (not (null where))
		       (multiple-value-list
			(apply where joined-list)))
		     (when (not (null order-by))
		       (multiple-value-list
			(apply order-by select-list)))
		     (when (not (null having))
		       (multiple-value-list
			(apply having joined-list)))
		     (when (not (null fetch))
		       (multiple-value-list
			(apply fetch fetch-references)))
		     limit
		     offset))))
