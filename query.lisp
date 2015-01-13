(in-package #:cl-db)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun compute-fetch-references (column-selector
				 &optional reference &rest references)
  (when (not (null reference))
    (multiple-value-bind (fetch-columns
			  fetch-from-clause
			  fetch-loader)
	(funcall reference column-selector)
      (multiple-value-bind (rest-fetch-columns
			    rest-fetch-from-clause
			    rest-fetch-loaders)
	  (apply #'compute-fetch-references
		 column-selector references)
	(values
	 (append fetch-columns rest-fetch-columns)
	 (remove-duplicates (append fetch-from-clause
				    rest-fetch-from-clause)
			    :from-end t)
	 (list* fetch-loader rest-fetch-loaders)))))) ;; loaders as alist

(defun make-subquery (select-list-items from-clause
		      where-clause order-by-clause
		      group-by-clause having-clause limit
		      offset references-to-fetch)
  (let* ((query-alias "main")
	 (select-list-index
	  (reduce #'(lambda (result select-list-item)
		      (multiple-value-bind (expression alias)
			  (funcall select-list-item)
			(acons expression
			       #'(lambda ()
				   (values alias query-alias))
			       result)))
		  select-list-items
		  :initial-value nil))
	 (expression-selector
	  #'(lambda (column-expression)
	      (assoc column-expression select-list-index)))
	 (select-list-items
	  (reduce #'append select-list-index :key #'rest))
	 (order-by-clause
	  (reduce #'(lambda (order-by-clause order-item)
		      (list* (funcall order-item expression-selector)
			     order-by-clause))
		  order-by-clause)))
    (multiple-value-bind (fetch-columns
			  fetch-from-clause fetch-loaders)
	(apply #'compute-fetch-references
	       #'(lambda (column-expression)
		   (assoc column-expression select-list-index))
	       references-to-fetch)
      (values (list :select (append select-list-items fetch-columns)
		    :from (list*
			   (list
			    (list :select select-list-items
				  :from from-clause
				  :where where-clause
				  :group-by group-by-clause
				  :having having-clause
				  :limit limit
				  :offset offset) :as query-alias)
			   fetch-from-clause)
		    :order-by order-by-clause)
	      fetch-loaders))))

(defun make-query (select-list-items from-clause
		   where-clause order-by-clause
		   group-by-clause having-clause limit
		   offset references-to-fetch)
  (let* ((expression-selector
	  #'(lambda (column-expression)
	      column-expression))
	 (order-by-clause
	  (mapcar #'(lambda (order-by-item)
		      (funcall order-by-item expression-selector))
		  order-by-clause)))
    (multiple-value-bind (fetch-columns
			  fetch-from-clause fetch-loaders)
	(apply #'compute-fetch-references
	       expression-selector references-to-fetch)
      (values (list :select (append select-list-items fetch-columns)
		    :from (append from-clause fetch-from-clause)
		    :where where-clause
		    :group-by group-by-clause
		    :order-by order-by-clause
		    :having having-clause
		    :limit limit
		    :offset offset)
	      fetch-loaders))))

(defun compute-clause (expression &rest rest-clause)
  (multiple-value-bind (result from-clause)
      (funcall expression)
    (multiple-value-bind (rest-result rest-from-clause)
	(when (not (null rest-clause))
	  (apply #'compute-clause rest-clause))
      (values
       (list* result rest-result)
       (append from-clause rest-from-clause)))))

(defun compute-group-by-clause (select-list-item
				&rest select-list-items)
  (multiple-value-bind (group-by-clause group-present-p)
      (when (not (null select-list-items))
	(apply #'compute-group-by-clause select-list-items))
    (multiple-value-bind (expression alias group-by-columns)
	(funcall select-list-item)
      (declare (ignore expression alias))
      (values (append group-by-columns group-by-clause)
	      (or (null group-by-columns)
		  group-present-p)))))

(defun compute-select-clause (select-item &rest select-list)
;;(multiple-value-bind (group-by-clause group-by-present-p)
;;	(apply #'compute-group-by-clause select-list-items)
  (multiple-value-bind (select-list-items
			select-from-clause
			loader
			fetched-refernces)
      (funcall select-item)
    (multiple-value-bind (rest-select-list-items
			  rest-select-from-clause
			  rest-loaders
			  rest-fetched-refernces)
	(when (not (null select-list))
	  (apply #'compute-select-clause select-list))
      (values
       (append select-list-items rest-select-list-items)
       (append select-from-clause rest-select-from-clause)
       (list* loader rest-loaders)
       (list* fetched-refernces rest-fetched-refernces)))))

(defun compute-query (joined-list select-list where-clause
		      order-by-clause having-clause
		      fetched-references limit offset)
  (multiple-value-bind (select-list-items
			select-list-from-clause loaders)
      (apply #'compute-select-clause select-list)
    (multiple-value-bind (group-by-clause group-by-present-p)
	(apply #'compute-group-by-clause select-list-items)
      (multiple-value-bind (where-clause where-from-clause)
	  (apply #'compute-clause where-clause)
	(multiple-value-bind (having-clause having-from-clause)
	    (apply #'compute-clause having-clause)
	  (let ((from-clause
		 (remove-duplicates (append select-list-from-clause
					    where-from-clause
					    having-from-clause)
				    :from-end t)))
	    (multiple-value-bind (query
				  fetched-references-loaders
				  placeholder-values)
		(if (not (null (and fetch (or limit offset))))
		    (make-subquery select-list-items from-clause
				   where-clause order-by-clause
				   (when group-by-present-p
				     group-by-clause)
				   having-clause
				   limit offset
				   references-to-fetch)
		    (make-query select-list-items from-clause
				where-clause order-by-clause
				(when group-by-present-p
				  group-by-clause)
				having-clause
				limit offset
				references-to-fetch))
	      (values query
		      (mapcar #'(lambda (select-list-item item-loader)
				  (let ((reference-loaders
					 (mapcar #'rest
						 (remove select-list-item
							 fetched-references-loaders
							 :test-not #'eq
							 :key #'first))))
				    #'(lambda (object object-rows)
					(funcall item-loader
						 object
						 object-rows
						 reference-loaders))))
			      loaders)
		      placeholder-values))))))))

(defun compute-query (joined-list select-list where-clause
		      order-by-clause having-clause
		      fetched-references limit offset)
  (multiple-value-bind (query loaders)
      (apply #'compute-select-clause select-list)
    (multiple-value-bind (where-clause where-from-clause)
	(apply #'compute-clause where-clause)
      (multiple-value-bind (having-clause having-from-clause)
	  (apply #'compute-clause having-clause)
	(let ((from-clause
	       (remove-duplicates (append select-list-from-clause
					  where-from-clause
					  having-from-clause)
				  :from-end t)))
	  (multiple-value-bind (query
				fetched-references-loaders
				placeholder-values)
	      (if (not (null (and fetch (or limit offset))))
		  (make-subquery select-list-items from-clause
				 where-clause order-by-clause
				 (when group-by-present-p
				   group-by-clause)
				 having-clause
				 limit offset
				 references-to-fetch)
		  (make-query select-list-items from-clause
			      where-clause order-by-clause
			      (when group-by-present-p
				group-by-clause)
			      having-clause
			      limit offset
			      references-to-fetch))
	    (values query
		    (mapcar #'(lambda (select-list-item item-loader)
				(let ((reference-loaders
				       (mapcar #'rest
					       (remove select-list-item
						       fetched-references-loaders
						       :test-not #'eq
						       :key #'first))))
				  #'(lambda (object object-rows)
				      (funcall item-loader
					       object
					       object-rows
					       reference-loaders))))
			    loaders)
		    placeholder-values))))))))
