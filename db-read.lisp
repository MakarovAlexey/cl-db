(in-package #:cl-db)

(defun make-context (name &key previous-context aux recursive where
			    order-by having select offset limit fetch)
  (make-instance 'context
		 :name name
		 :previous-context previous-context
		 :aux-clause aux
		 :recursive-clause recursive
		 :select-list select
		 :where-clause where
		 :order-by-clause order-by
		 :having-clause having
		 :limit-clause limit
		 :offset-clause offset
		 :fetch-clause fetch))

(defun ensure-recursive-joining (aux-clause recursive-clause)
  (when (not (null recursive-clause))
    (make-context "recursive_joining"
		  :aux aux-clause
		  :recursive recursive-clause)))

(defun make-selection (previous-context select-list where-clause
		       order-by-clause having-clause limit offset
		       fetch-clause recursive-clause)
  (let ((recursive-fetch
	 (remove-if #'null fetch-clause :key #'recursive-node-of)))
    (if (or (not (null recursive-clause))
	    (not (null recursive-fetch))
	    (and (not (null fetch-clause))
		 (or (not (null offset))
		     (not (null limit)))))
	(let ((selection
	       (make-context "selection"
			     :previous-context previous-context
			     :select select-list
			     :where where-clause
			     :order-by order-by-clause
			     :having having-clause
			     :offset offset
			     :limit limit)))
	  (if (not (null recursive-fetch))
	      (make-context "main" :previous-context
			    (make-context "fetching"
					  :previous-context selection
					  :select select-list
					  :fetch fetch-clause
					  :recursive recursive-fetch)
			    :select select-list
			    :fetch fetch-clause
			    :order-by order-by-clause)
	      (make-context "main"
			    :previous-context selection
			    :select select-list
			    :fetch fetch-clause
			    :order-by order-by-clause)))
	(make-context "selection"
		      :previous-context previous-context
		      :select select-list
		      :where where-clause
		      :order-by order-by-clause
		      :having having-clause
		      :offset offset
		      :limit limit
		      :fetch fetch-clause))))

(defun compute-clause (clause args &optional default)
  (if (not (null clause))
      (multiple-value-list
       (apply clause args))
      default))

(defun make-query (class-names join &key select aux recursive where
				      order-by having fetch limit offset)
  (let* ((selectors (if (listp class-names)
			(mapcar #'(lambda (class-name)
				    (make-root class-name))
				class-names)
			(list (make-root class-names))))
	 (join-list (reduce #'append (compute-clause join selectors)
			    :initial-value selectors))
	 (aux-clause (compute-clause aux join-list))
	 (recursive-clause (compute-clause recursive join-list))
	 (select-list (compute-clause select join-list selectors))
	 (where-clause (compute-clause where join-list))
	 (order-by-clause (compute-clause order-by join-list))
	 (having-clause (compute-clause having select-list))
	 (fetch-clause (when (not (null fetch))
			 (reduce #'append (multiple-value-list
					   (apply fetch select-list)))))
	 (*table-index* 0))
    (make-selection (ensure-recursive-joining aux-clause recursive-clause)
		    select-list where-clause order-by-clause
		    having-clause limit offset fetch-clause
		    recursive-clause)))
		
(defun db-read (roots &key join aux recursive where order-by having
			select fetch singlep offset limit transform)
  (declare (ignore transform singlep))
  (let ((context (make-query roots join
			     :aux aux
			     :recursive recursive
			     :select select
			     :where where
			     :order-by order-by
			     :having having
			     :offset offset
			     :limit limit
			     :fetch fetch)))
    (execute-query (connection-of *session*)
		   (compile-sql-query context))))
