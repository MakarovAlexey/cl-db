(in-package #:cl-db)

(defun property (reader entity)
  (funcall entity reader))
		    
(defun join (references accessor alias &optional join)
  (multiple-value-bind (selector references)
      (funcall references accessor)
    (list* alias selector
	   (when (not (null join))
	     (funcall join references)))))

(defun fetch (references accessor &optional fetch)
  (multiple-value-bind (selector references)
      (funcall references accessor 'fetch)
    (list* selector
	   (when (not (null fetch))
	     (funcall fetch references)))))

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
	(compute-query joined-list
		       (if (not (null select))
			   (multiple-value-list
			    (apply select joined-list))
			   selectors)
		       where order-by having limit offset fetch)))))
