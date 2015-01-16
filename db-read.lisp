(in-package #:cl-db)

(defun compute-select (select-item &rest select-list)
  (multiple-value-bind (selector fetched-refernces)
      (funcall select-item)
    (multiple-value-bind (selectors rest-fetched-refernces)
	(when (not (null select-list))
	  (apply #'compute-select select-list))
      (values
       (list* selector selectors)
       (list* fetched-refernces rest-fetched-refernces)))))

(defun property (reader entity)
  (funcall entity reader))
		    
(defun join (references accessor alias &optional join)
  (multiple-value-bind (selector references)
      (funcall references accessor)
    (list* alias selector
	   (when (not (null join))
	     (multiple-value-list
	      (funcall join references))))))

(defun fetch (references accessor &optional fetch)
  (let ((reference-fn (funcall references accessor)))
    #'(lambda (query)
	(multiple-value-bind (columns
			      from-clause
			      loader
			      references)
	    (funcall reference-fn query)
	  (multiple-value-bind (reference-columns
				reference-from-clause
				reference-loaders)
	      (when (not (null fetch))
		(apply #'compute-fetch
		       (multiple-value-list
			(funcall fetch references))))
	    (values (append columns reference-columns)
		    (append from-clause reference-from-clause)
		    #'(lambda (&rest args)
			(apply loader (append args reference-loaders)))
		    references))))))

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
	    (compute-select
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
