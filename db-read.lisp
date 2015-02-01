(in-package #:cl-db)

(defun binary-operator (operator lhs-expression
			rhs-expression &rest rest-expressions)
  (let ((expression (list*
		     (expression-of lhs-expression)
		     operator
		     (expression-of rhs-expression)
		     (reduce #'(lambda (expression result)
				 (list* operator expression result))
			     rest-expressions
			     :key #'expression-of
			     :initial-value nil
			     :from-end t)))
	(alias (make-alias "op")))
    (make-expression :expression expression
		     :count expression
		     :select-list (cons expression alias)
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
  (apply #'binary-operator :and
	 lhs-expression
	 rhs-expression
	 rest-expressions))

(defun db-or (lhs-expression rhs-expression &rest rest-expressions)
  (apply #'binary-operator :or
	 lhs-expression
	 rhs-expression
	 rest-expressions))

(defun db-not (expression)
  (let ((expression (list* :not (expression-of expression)))
	(alias (make-alias "op")))
    (make-expression :expression expression
		     :count expression
		     :select-list (cons expression alias)
		     :from-clause (from-clause-of expression)
		     :group-by-clause (group-by-clause-of expression)
		     :loader (make-value-loader alias))))

;; Comparison Operators

(defun db-< (lhs-expression rhs-expression)
  (binary-operator :< lhs-expression rhs-expression))

(defun db-> (lhs-expression rhs-expression)
  (binary-operator :> lhs-expression rhs-expression))

(defun db-<= (lhs-expression rhs-expression)
  (binary-operator :<= lhs-expression rhs-expression))

(defun db->= (lhs-expression rhs-expression)
  (binary-operator :>= lhs-expression rhs-expression))

(defun db-eq (lhs-expression rhs-expression)
  (binary-operator := lhs-expression rhs-expression))

(defun db-not-eq (lhs-expression rhs-expression)
  (binary-operator :<> lhs-expression rhs-expression))

(defun db-is-null (lhs-expression)
  (binary-operator :is lhs-expression :null))

(defun db-is-true (lhs-expression)
  (binary-operator :is lhs-expression :true))

(defun db-is-false (lhs-expression)
  (binary-operator :is lhs-expression :false))

(defun db-between (expression lhs-expression rhs-expression)
  (let* ((range (db-and lhs-expression rhs-expression))
	 (expression (list* (expression-of expression) :between range))
	 (alias (make-alias "op")))
    (make-expression :expression expression
		     :count expression
		     :select-list (list expression)
		     :from-clause (append
				   (from-clause-of expression)
				   (from-clause-of range))
		     :group-by-clause (append
				       (group-by-clause-of expression)
				       (group-by-clause-of range))
		     :loader (make-value-loader alias))))

(defun compute-select (select-item &rest select-list)
  (multiple-value-bind (selectors rest-fetched-refernces)
      (when (not (null select-list))
	(apply #'compute-select select-list))
    (values
     (list* select-item selectors)
     (list* (fetch-references-of select-item) rest-fetched-refernces))))

(defun property (mapping reader)
  (funcall (properties-of mapping) reader))
		    
(defun join (references accessor alias &optional join)
  (multiple-value-bind (selector references)
      (funcall references accessor)
    (list* alias selector
	   (when (not (null join))
	     (multiple-value-call #'append
	      (funcall join references))))))

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
  (let ((*table-index* 0)
	(*mapping-schema* mapping-schema))
    (multiple-value-bind (selectors joined-references)
	(if (not (listp roots))
	    (make-join-plan mapping-schema roots)
	    (apply #'make-join-plan mapping-schema roots))
      (let ((joined-list
	     (append selectors
		     (when (not (null join))
		       (multiple-value-call #'append
			 (apply join joined-references))))))
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
			 offset))))))
