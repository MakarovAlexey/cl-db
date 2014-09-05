(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (prefix "table"))
  (format nil "~a_~a" prefix (incf *table-index*)))

(defun append-alias (alias &rest columns)
  (mapcar #'(lambda (column-name)
	      (format nil "~a.~a" alias column-name))
	  columns))

(defun plan-primary-key (alias &rest primary-key)
  (apply #'append-alias alias primary-key))

(defun plan-property (alias &key slot-name columns)
  (list :slot-name slot-name
	:columns (apply #'append-alias alias columns)))

(defun plan-inheritance (root-alias &rest class-mapping
			 &key (alias (make-alias))
			   superclass-mappings &allow-other-keys)
  (list* :alias alias
	 :root-alias root-alias
	 :superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'plan-inheritance
			    alias superclass-mapping))
		 superclass-mappings)
	 (alexandria:remove-from-plist class-mapping
				       :superclass-mappings)))

(defun plan-extension (root-alias &rest class-mapping)
  (list* :root-alias root-alias
	 (apply #'make-join-plan class-mapping)))

(defun multiple-value-reduce (function list &key initial-values)
  (apply function
	 (reverse
	  (list*
	   (first list)
	   (reverse
	    (if (not (null (rest list)))
		(multiple-value-list
		 (multiple-value-reduce function
					(rest list)
					:initial-values initial-values))
		initial-values))))))

;; FIXME: introduce macrolet
(defun reduce-superclass-mappings (function superclass-mappings
				   &key initial-values
				     &allow-other-keys)
  (multiple-value-reduce
   #'(lambda (result superclass-mapping)
       (funcall function
		(apply #'reduce-superclass-mappings function
		       :initial-value result
		       superclass-mapping)
		superclass-mapping))
   superclass-mappings
   :initial-values initial-values))

(defun reduce-subclass-mappings (function subclass-mappings
				 &key initial-values
				   &allow-other-keys)
  (multiple-value-reduce
   #'(lambda (result subclass-mapping)
       (funcall function
		(apply #'reduce-subclass-mappings function
		       :initial-value result
		       subclass-mapping)
		subclass-mapping))
   subclass-mappings
   :initial-values initial-values))

;; '((:table-name "fff" :class-name fdefefef)
;;   :superclass-mappings
;;   :subclass-mappings

(defun make-join-plan (&rest class-mapping &key subclass-mappings
					     superclass-mappings)
  (let ((subclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'make-join-plan superclass-mapping))))
	(superclass-mappings
	 (mapcar #'(lambda (superclass-mapping)
		     (apply #'make-join-plan superclass-mapping))))
	(alias (make-alias)))
    #'(lambda (&rest function)
	(apply #'(lambda 
	       #'(lambda (function &rest args &key superclass-mappings)
		   (funcall function
			    :superclass-mappings
			    
				    su
  (reduce-subclass-mapping #'(lambda (result &rest class-mapping)
			       (list* :alias (make-alias)
   

;; FIXME: append alias to columns names; build in place of foreign key
;; SQL "on" construction
(defun make-join-plan (&key (alias (make-alias))
			 primary-key
			 properties
			 superclass-mappings
			 subclass-mappings
			 &allow-other-keys)
  (list :class-name class-name
	:table-name table-name
	:alias alias
	:primary-key (apply #'append-alias alias primary-key)
	:properties properties
	:superclass-mappings
	(mapcar #'(lambda (superclass-mapping)
		    (apply #'plan-inheritance alias superclass-mapping))
		superclass-mappings)
	:subclass-mappings
	(mapcar #'(lambda (subclass-mapping)
		    (apply #'plan-extension alias subclass-mapping))
		subclass-mappings)))

;; FIXME: select list, where clause, order by, having
(defun make-sql-query (&key table-name alias primary-key
			 superclass-mappings subclass-mappings
			 &allow-other-keys)
  (list*
   (list :from table-name :as alias)
   (list*
    (reduce-subclass-mappings
     #'(lambda (result superclass-primary-key
		&key table-name alias primary-key foreign-key
		  superclass-mappings)
	 (values
	  (list*
	   (list*
	    (list :left-join table-name :as alias
		  :on (pairlis superclass-primary-key
			       (apply #'append-alias
				      alias foreign-key)))
	    (reduce-superclass-mappings
	     #'(lambda (result subclass-foreign-key
			&key table-name alias primary-key foreign-key)
		 (values
		  (list*
		   (list :inner-join table-name :as alias
			 :on (pairlis subclass-foreign-key
				      (apply #'append-alias
					     alias primary-key)))
		  result)
		  (apply #'append-alias alias foreign-key)))
	     superclass-mappings
	     :initial-values (list nil nil))
	    result))
	  (apply #'append-alias alias primary-key)))
     subclass-mappings)
    :initial-values (list nil (apply #'append-alias alias primary-key)))))

;;(defun make-loaders (&rest class-mapping &key superclass-mappings
;;					   subclass-mappings)
;;  (run-subclass-mappings
;;   #'(lambda (loaders &key class-name alias primary-key
;;			properties superclass-mappings) ;; subclasses
;;       (list*
;;	(make-loader class-name alias primary-key
;;		     (list* properties
;;			    (run-superclass-mappings
;;			     #'(lambda (properties &key alias) ;; superclasses
;;				 (list*
;;				  (mapcar #'(lambda (property)
;;					      (apply #'comute-property-loader
;;						     alias property))
;;					  properties)
;;				  properties))
;;			     class-mapping)))
;;	loaders))
;;   (list class-mapping)))

(defun make-query (class-mapping)
  (values (apply #'make-sql-query class-mapping)))

;;(defun from-clause (&key table-name alias superclass-mappings
;;		      subclass-mappings &allow-other-keys)
;;  (format nil (concatenate "FROM ~a AS ~a~%"
;;			   "~{~\print-superclass-join~\~}~%"
;;			   "~{~\print-subclass-join~\~}")
;;	  table-name alias superclass-mappings subclass-mappings))

;;(defun make-query (&rest class-mapping &key superclass-mappings
;;					 subclass-mappings
;;					 &allow-other-keys)
;;  (let ((join-plan (apply #'make-join-plan class-mapping)))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (find class-name mapping-schema
		:key #'(lambda (class-mapping)
			 (getf class-mapping :class-name))))
	 (*table-index* 0))
    (when (null class-mapping)
      (error "Mapping for class ~a not found" class-name))
    (apply #'make-join-plan class-mapping)))
