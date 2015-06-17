(in-package #:cl-db)

(defun compute-select-clause (select-item &rest select-list)
  (multiple-value-bind (query loaders)
      (when (not (null select-list))
	(apply #'compute-select-clause select-list))
    (values
     (query-append query
		   :select-list (select-list-of select-item)
		   :from-clause (from-clause-of select-item)
		   :group-by-clause (group-by-clause-of select-item))
     (list* (loader select-item) loaders))))

(defun append-where-clause (query &optional expression
			    &rest rest-clause)
  (if (not (null expression))
      (query-append (apply #'append-where-clause query rest-clause)
		    :where-clause (expression-of expression)
		    :from-clause (from-clause-of expression))
      query))

(defun append-having-clause (query &optional expression
			     &rest rest-clause)
  (if (not (null expression))
      (query-append (apply #'append-where-clause query rest-clause)
		    :having-clause (expression-of expression)
		    :from-clause (from-clause-of expression))
      query))

(defun append-fetch-expressions (query loader fetch-expressions
				 &rest rest-expressions)
  (multiple-value-bind (query loader)
      (if (not (null rest-expressions))
	  (apply #'append-fetch-expressions
		 query rest-expressions)
	  (values query loader))
    (apply #'compute-fetch query loader fetch-expressions)))

(defun wrap-query (query)
  (multiple-value-bind (query-select-list
			query-from-clause
			query-where-clause
			query-group-by-clause
			query-having-clause
			query-order-by
			query-limit
			query-offset)
      (funcall query)
    (let* ((query-alias "main")
	   (select-list
	    (reduce #'(lambda (result select-list-item)
			(let ((alias (rest select-list-item)))
			  (acons (list #'write-column
				       alias query-alias)
				 alias
				 result)))
		    query-select-list
		    :initial-value nil)))
      #'(lambda (&optional column-expression)
	  (if (not (null column-expression))
	      (rassoc column-expression select-list)
	      (values select-list
		      (list
		       (list #'write-subquery
			     (make-query-expression query-select-list
						    query-from-clause
						    query-where-clause
						    query-group-by-clause
						    query-having-clause
						    nil
						    query-limit
						    query-offset)
			     query-alias))
		      nil
		      nil
		      nil
		      query-order-by))))))

(defun append-fetch-clause (query loaders limit offset
			    fetch-expressions)
  (if (not (null fetch-expressions))
      (apply #'append-fetch-expressions
	     (if (not (null (or limit offset)))
		 (wrap-query query)
		 query)
	     (reduce #'(lambda (result loader)
			 (list* loader
				(mapcar #'first
					(remove loader
						fetch-expressions
						:test-not #'eq
						:key #'rest))
				result))
		     loaders :initial-value nil))
      (values query loaders)))

(defun append-order-by-clause (query &optional order-by-expression
			       &rest order-by-clause)
  (if (not (null order-by-expression))
      (query-append (apply #'append-order-by-clause
			   query order-by-clause)
		    :order-by-clause
		    (funcall order-by-expression query))
      query))

(defun make-query-expression (select-list from-clause where-clause
			      group-by-clause having-clause
			      order-by-clause limit offset)
  (remove nil
	  (list
	   (list* #'write-select-list
		  (reduce #'(lambda (result select-list-item)
			      (list* (list #'write-label
					   (first select-list-item)
					   (rest select-list-item))
				     result))
			  select-list :initial-value nil))
	   (list* #'write-from-clause from-clause)
	   (when (not (null where-clause))
	     (list* #'write-where-clause where-clause))
	   (when (some #'null group-by-clause)
	     (list* #'write-group-by-clause
		    (reduce #'append group-by-clause)))
	   (when (not (null having-clause))
	     (list* #'write-having-clause having-clause))
	   (when (not (null order-by-clause))
	     (list* #'write-order-by-clause order-by-clause))
	   (when (not (null limit))
	     (list #'write-limit limit))
	   (when (not (null offset))
	     (list #'write-offset offset)))))

(defun compute-query (select-list where-clause order-by-clause
		      having-clause fetch-clause limit offset)
  (multiple-value-bind (query loaders)
      (apply #'compute-select-clause select-list)
    (let ((query
	   (query-append (apply #'append-having-clause
				(apply #'append-where-clause
				       query where-clause)
				having-clause)
			 :limit limit :offset offset))
	  (fetch-expressions
	   (reduce #'(lambda (result fetch-expression)
		       (multiple-value-call #'acons
			 (funcall fetch-expression) result))
		   fetch-clause :initial-value nil)))
      (multiple-value-bind (query loaders)
	  (append-fetch-clause query loaders limit offset
			       fetch-expressions)
	(multiple-value-bind (sql-string parameters)
	    (make-sql-string
	     (multiple-value-call #'make-query-expression
	       (funcall
		(apply #'append-order-by-clause
		       query order-by-clause))))
	  (values sql-string parameters loaders))))))

(defgeneric parse-expression (element &optional result))

(defmethod parse-expression ((element list) &optional result)
  (append
   (list* :begin (reduce #'parse-element element
			 :from-end t :initial-value result))
   (list* :end result)))

(defmethod parse-expression ((element t) &optional result)
  (list* element result))

(defun expression-lexer (expression)
  (let ((parsed-expression
	 (parse-expression expression)))
  #'(lambda ()
      (let ((value (pop parsed-expression)))
	(when (not (null value))
	  (values
	   (cond
	     ((keywordp value) value)
	     ((symbolp value) :symbol)
	     (t :parameter))
	   value))))))

(yacc:define-parser *expression-parser*
  (:start-symbol form)
  (:terminals (:begin :end :symbol :parameter :property
		      :and :or :not :null := :eq :eql :equal :equalp
		      :< :> :<= :>= :<> :!= :+ :- :* :/
		      :abs :sqrt :exp :floor :log
		      :mod :pi :expt :round :truncate
		      :acos :asin :atan :atan2 :cos :sin :tan
		      :like
		      :ascending :descending))
  
  (form
   (:begin expression :end #'(lambda (begin expression end)
			       (declare (ignore begin end))
			       expression))
   (:begin postfix-operation :end #'(lambda (begin expression end)
				      (declare (ignore begin end))
				      expression))
   :parameter)
  
  (postfix-operation
   (:null form #'(lambda (null form)
		   (declare (ignore null))
		   `(make-instance 'is-null :expression ,form)))
   (:not :begin :null form :end #'(lambda (not begin null form end)
				    (declare (ignore not begin null end))
				    `(make-instance 'is-not-null :expression ,form))))
  
  (expression binary-operation binary-operation-extended
	      function-call property sort)

  (sort
   (direction form #'(lambda (direction expression)
		       `(make-instance (quote ,direction)
				       :expression (quote ,expression)))))

  (direction
   (:ascending #'(lambda (op)
		   (declare (ignore op))
		   'ascending))
   (:descending #'(lambda (op)
		    (declare (ignore op))
		    'descending)))

  (property
   (:symbol :symbol #'(lambda (reader class-mapping)
			`(property ,class-mapping
				   (function ,reader)))))
  
  (binary-operation comparison pattern-matching)

  (pattern-matching
   (:like object :parameter #'(lambda (like object pattern)
				(declare (ignore like))
				`(make-instance 'like
						:lhs-expression ,object
						:rhs-expression ,pattern))))

  (comparison
   (comparison-operator form form #'(lambda (op lhs rhs)
				      `(make-instance (quote ,op)
						      :lhs-expression ,lhs
						      :rhs-expression ,rhs))))

  (comparison-operator
   (equal #'(lambda (op)
	      (declare (ignore op))
	      'equal))
   (:< #'(lambda (op)
	   (declare (ignore op))
	   'less-than))
   (:> #'(lambda (op)
	   (declare (ignore op))
	   'more-than))
   (:<= #'(lambda (op)
	   (declare (ignore op))
	   'less-than-or-equal))
   (:>= #'(lambda (op)
	    (declare (ignore op))
	    'more-than-or-equal))
   (:<> #'(lambda (op)
	    (declare (ignore op))
	    'not-equal))
   (:!= #'(lambda (op)
	    (declare (ignore op))
	    'not-equal)))

  (equal := :eq :eql :equal :equalp)

  (binary-extended-operator
   (:and #'(lambda (op)
	    (declare (ignore op))
	    'and-operation))
   (:or #'(lambda (op)
	    (declare (ignore op))
	    'or-operation))
   (:+ #'(lambda (op)
	    (declare (ignore op))
	    'addition))
   (:- #'(lambda (op)
	    (declare (ignore op))
	    'subtraction))
   (:* #'(lambda (op)
	    (declare (ignore op))
	    'multiplication))
   (:/ #'(lambda (op)
	    (declare (ignore op))
	    'division)))

  (binary-operation-extended
   (binary-extended-operator forms #'(lambda (op forms)
				       `(make-instance (quote ,op)
						       :lhs-expression (first (quote ,forms))
						       :reast-expressions (rest (quote forms))))))
  
  (forms
   (form forms)
   (form))

  (sql-function-call function-call aggregation)
  
  (function-call
   (sql-function arguments #'(lambda (sql-function arguments)
			       `(make-instance (quote ,sql-function)
					       :argumets (quote ,arguments)))))
  
  (sql-function
   (:abs #'(lambda (op)
	    (declare (ignore op))
	    'abs))
   (:sqrt #'(lambda (op)
	      (declare (ignore op))
	      'sqrt))
   (:exp #'(lambda (op)
	    (declare (ignore op))
	    'exp))
   (:floor #'(lambda (op)
	       (declare (ignore op))
	       'floor))
   (:log #'(lambda (op)
	    (declare (ignore op))
	    'log))
   (:mod #'(lambda (op)
	    (declare (ignore op))
	    'mod))
   (:pi #'(lambda (op)
	    (declare (ignore op))
	    'pi))
   (:expt #'(lambda (op)
	      (declare (ignore op))
	      'power))
   (:round #'(lambda (op)
	       (declare (ignore op))
	       'round))
   (:truncate #'(lambda (op)
		  (declare (ignore op))
		  'trunc))
   (:acos #'(lambda (op)
	    (declare (ignore op))
	    'acos))
   (:asin #'(lambda (op)
	      (declare (ignore op))
	      'asin))
   (:atan #'(lambda (op)
	      (declare (ignore op))
	      'atan))
   (:cos #'(lambda (op)
	    (declare (ignore op))
	    'cos))
   (:sin #'(lambda (op)
	     (declare (ignore op))
	     'sin))
   (:tan #'(lambda (op)
	     (declare (ignore op))
	     'tan)))

  (arguments
   (form arguments)
   form)
  
  (aggregation (aggregate-function form))
  
  (aggregate-function
   (:avg #'(lambda (op)
	     (declare (ignore op))
	     'avg))
   (:count #'(lambda (op)
	     (declare (ignore op))
	     'count))
   (:every #'(lambda (op)
	     (declare (ignore op))
	     'count))
   (:max #'(lambda (op)
	     (declare (ignore op))
	     'max))
   (:min #'(lambda (op)
	     (declare (ignore op))
	     'min))
   (:sum #'(lambda (op)
	     (declare (ignore op))
	     'max))))

(defmacro expressions ((&rest args) &body body)
  `(values ,(mapcar #'parse-expression (quote body))))
