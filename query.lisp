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

(defun query-append (query &key select-list from-clause where-clause
			     having-clause order-by-clause limit offset)
  (multiple-value-bind (query-select-list
			query-from-clause
			query-where-clause
			query-group-by-clause
			query-having-clause
			query-order-by
			query-limit
			query-offset)
      (when (not (null query))
	(funcall query))
    #'(lambda (&optional expression)
	(if (not (null expression))
	    expression
	    (values
	     (append query-select-list select-list)
	     (remove-duplicates (append query-from-clause
					from-clause)
				:from-end t)
	     (if (not (null where-clause))
		 (list* where-clause query-where-clause)
		 query-where-clause)
	     (append query-group-by-clause
		     (apply #'compute-group-by-clause select-list))
	     (if (not (null having-clause))
		 (list* having-clause query-having-clause)
		 query-having-clause)
	     (if (not (null order-by-clause))
		 (list* order-by-clause query-order-by)
		 query-order-by)
	     (or limit query-limit)
	     (or offset query-offset))))))

(defun compute-select-clause (select-item &rest select-list)
  (multiple-value-bind (query loaders)
      (when (not (null select-list))
	(apply #'compute-select-clause select-list))
    (multiple-value-bind (select-list-items select-from-clause loader)
	(funcall select-item)
      (values
       (query-append query
		     :select-list select-list-items
		     :from-clause select-from-clause)
       (list* loader loaders)))))

(defun append-where-clause (query &optional expression
			    &rest rest-clause)
  (if (not (null expression))
      (multiple-value-bind (where-expression from-clause)
	  (funcall expression)
	(query-append (apply #'append-where-clause
			     query rest-clause)
		      :where-clause where-expression
		      :from-clause from-clause))
      query))

(defun append-having-clause (query &optional expression
			     &rest rest-clause)
  (if (not (null expression))
      (multiple-value-bind (having-expression from-clause)
	  (funcall expression)
	(query-append (apply #'append-having-clause
			     query rest-clause)
		      :having-clause having-expression
		      :from-clause from-clause))
      query))

(defun append-fetch-expressions (query loader
				 &optional fetch-expressions
				 &rest rest-expressions)
  (if (not (null fetch-expressions))
      (multiple-value-bind (query loader)
	  (apply #'append-fetch-expressions
		 query loader rest-expressions)
	(apply #'compute-fetch query loader fetch-expressions))
      (values query loader)))

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
	 (select-list-index
	  (reduce #'(lambda (result select-list-item)
		      (multiple-value-bind (expression alias)
			  (funcall select-list-item)
			(acons expression
			       #'(lambda ()
				   (values alias query-alias))
			       result)))
		  query-select-list
		  :initial-value nil)))
    #'(lambda (&optional column-expression)
	(if (not (null column-expression))
	    (assoc column-expression select-list-index)
	    (values (reduce #'append select-list-index :key #'rest)
		    (list* (list :select query-select-list
				 :from query-from-clause
				 :where query-where-clause
				 :group-by query-group-by-clause
				 :having query-having-clause
				 :limit query-limit
				 :offset query-offset)
			   :as query-alias)
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
					(remove loader fetch-expressions
						:test-not #'eq :key #'rest))
				result))
		     loaders :initial-value nil))
      (values query loaders)))

(defun append-order-by-clause (query &optional order-by-expression
			       &rest order-by-clause)
  (if (not (null order-by-expression))
      (query-append (apply #'append-order-by-clause
			   query order-by-clause)
		    :order-by-clause (funcall order-by-clause))
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
	(values (append-order-by-clause query order-by-clause)
		loaders)))))
