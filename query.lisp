(in-package #:cl-db)

(defun compute-group-by-clause (select-list-item
				&rest select-list-items)
  (multiple-value-bind (group-by-clause)
      (when (not (null select-list-items))
	(apply #'compute-group-by-clause select-list-items))
    (multiple-value-bind (expression alias group-by-columns)
	(funcall select-list-item)
      (declare (ignore expression alias))
      (append group-by-columns group-by-clause))))

(defun compute-select-clause (select-item &rest select-list)
  (multiple-value-bind (query loaders)
      (when (not (null select-list))
	(apply #'compute-select-clause select-list))
    (values
     (query-append query
		   :select-list (select-list-of select-item)
		   :from-clause (from-clause-of select-item)
		   :group-by-clause (list (group-by-clause-of select-item)))
     (list* (funcall select-item :loader) loaders))))

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

(defun wrap-query (query limit offset)
  (multiple-value-bind (query-select-list
			query-from-clause
			query-where-clause
			query-group-by-clause
			query-having-clause
			query-order-by
			query-limit
			query-offset)
      (funcall query)
    (declare (ignore query-limit query-offset))
    (let* ((query-alias "main")
	   (select-list-index
	    (reduce #'(lambda (result select-list-item)
			(let ((alias (rest select-list-item)))
			  (acons (first select-list-item)
				 (cons (list :column alias query-alias)
				       alias)
				 result)))
		    query-select-list
		    :initial-value nil)))
      #'(lambda (&optional column-expression)
	  (if (not (null column-expression))
	      (rest
	       (assoc column-expression select-list-index))
	      (values (mapcar #'rest select-list-index)
		      (list (list
			     (list :select query-select-list
				   :from query-from-clause
				   :where query-where-clause
				   :group-by query-group-by-clause
				   :having query-having-clause
				   :limit limit
				   :offset offset)
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
		 (wrap-query query limit offset)
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
		    :order-by-clause (funcall order-by-expression query))
      query))

(defun compute-query (select-list where-clause order-by-clause
		      having-clause fetch-clause limit offset)
  (multiple-value-bind (query loaders)
      (apply #'compute-select-clause select-list)
    (let ((query (apply #'append-having-clause
			(apply #'append-where-clause
			       query
			       where-clause)
			having-clause))
	  (fetch-expressions
	   (reduce #'(lambda (result fetch-expression)
		       (multiple-value-call #'acons
			 (funcall fetch-expression) result))
		   fetch-clause :initial-value nil)))
      (multiple-value-bind (query loaders)
	  (append-fetch-clause query loaders limit offset
			       fetch-expressions)
	(values (apply #'append-order-by-clause
		       query order-by-clause)
		loaders)))))
