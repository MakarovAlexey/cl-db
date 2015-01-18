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
    #'(lambda ()
	(values (append query-select-list select-list)
		(remove-duplicates (append query-from-clause
					   from-clause)
				   :from-end t)
		(list* where-clause query-where-clause)
		(append query-group-by-clause
			(apply #'compute-group-by-clause select-list))
		(list* having-clause query-having-clause)
		(list* order-by-clause query-order-by)
		(or limit query-limit)
		(or offset query-offset)))))

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

(defun append-where-clause (query expression &rest rest-clause)
  (multiple-value-bind (where-expression from-clause)
      (funcall expression)
    (query-append (if (not (null rest-clause))
		      (apply #'append-where-clause
			     query rest-clause)
		      query)
		  :where-clause where-expression
		  :from-clause from-clause)))

(defun append-having-clause (query expression &rest rest-clause)
  (multiple-value-bind (having-expression from-clause)
      (funcall expression)
    (query-append (if (not (null rest-clause))
		      (apply #'append-having-clause
			     query rest-clause)
		      query)
		  :having-clause having-expression
		  :from-clause from-clause)))

(defun append-fetch-expressions (query fetch-expression &rest fetch)
  (multiple-value-bind (fetch-query reference-loaders-by-select-items)
      (when (not (null fetch))
	(apply #'append-fetch-expressions query fetch))
    (multiple-value-bind (select-list
			  from-clause
			  loader
			  select-list-element)
	(funcall fetch-expression fetch-query)
      (values (query-append query
			    :select-list select-list
			    :from-clause from-clause)
	      (acons select-list-element
		     loader
		     reference-loaders-by-select-items)))))

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

(defun append-fetch-clause (query limit offset fetch)
  (if (not (null fetch))
      (apply #'append-fetch-expressions
	     (if (not (null (or limit offset)))
		 (wrap-query query)
		 query)
	     fetch)
      query))

(defun append-order-by-clause (query &optional order-by-expression
			       &rest order-by-clause)
  (if (not (null order-by-expression))
      (query-append (apply #'append-order-by-clause
			   query order-by-clause)
		    :order-by-clause (funcall order-by-clause))
      query))

(defun compute-query (select-list where-clause order-by-clause
		      having-clause fetched-references limit offset)
  (multiple-value-bind (query loaders)
      (apply #'compute-select-clause select-list)
    (let ((query (apply #'append-having-clause
			(apply #'append-where-clause
			       query
			       where-clause)
			having-clause)))
      (multiple-value-bind (query reference-loaders-by-select-items)
	  (append-fetch-clause query limit offset fetched-references)
	(values
	 (append-order-by-clause query order-by-clause)
	 (mapcar #'(lambda (select-list-item item-loader)
		     (let ((reference-loaders
			    (mapcar #'rest
				    (remove select-list-item
					    reference-loaders-by-select-items
					    :test-not #'eq
					    :key #'first))))
		       #'(lambda (row result-set)
			   (funcall item-loader
				    row result-set reference-loaders))))
		 select-list
		 loaders))))))
