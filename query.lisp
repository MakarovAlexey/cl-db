(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (name "table"))
  (format nil "~a_~a" name (incf *table-index*)))

(defun append-alias (alias &rest columns)
  (mapcar #'(lambda (column)
	      (format nil "~a.~a" alias column))
	  columns))

(defun plan-superclass-mappings (alias &optional superclass-mapping
				 &rest superclass-mappings)
  (when (not (null superclass-mapping))
    (destructuring-bind ((class-name
			  (table-name primary-key
				      &rest properties)
			  &rest superclass-mappings)
			 &rest foreign-key)
	superclass-mapping
      (declare (ignore class-name properties))
      (let ((foreign-key
	     (apply #'append-alias alias foreign-key))
	    (alias (make-alias)))
	(format *from-clause*
		" INNER JOIN ~a AS ~a~%    ON ~{~a = ~a~^~%   AND ~}"
		table-name alias
		(alexandria:alist-plist
		 (pairlis foreign-key
			  (apply #'append-alias
				 alias primary-key))))
	(apply #'plan-superclass-mappings alias superclass-mappings)))
    (apply #'plan-superclass-mappings alias superclass-mappings)))

(defun plan-subclass-mappings (superclass-primary-key
			       &optional subclass-mapping
			       &rest subclass-mappings)
  (when (not (null subclass-mapping))
    (destructuring-bind (((class-name
			   (table-name primary-key &rest properties)
			   &rest superclass-mappings)
			  &rest subclass-mappings)
			 &rest foreign-key)
	subclass-mapping
      (declare (ignore class-name properties))
      (let ((alias (make-alias)))
	(format *from-clause*
		"  LEFT JOIN ~a AS ~a~%    ON ~{~a = ~a~^~%   AND ~}"
		table-name alias
		(alexandria:alist-plist
		 (pairlis superclass-primary-key
			  (apply #'append-alias
				 alias foreign-key))))
	(apply #'plan-superclass-mappings alias superclass-mappings)
	(apply #'plan-subclass-mappings
	       (apply #'append-alias alias primary-key)
	       subclass-mappings)))
    (apply #'plan-subclass-mappings
	   superclass-primary-key subclass-mappings)))

(defun plan-class-mapping (alias class-mapping)
  (destructuring-bind ((class-name
			(table-name primary-key &rest properties)
			&rest superclass-mappings)
		       &rest subclass-mappings)
      class-mapping
    (declare (ignore class-name properties))
    (mapcar #'(lambda (property)
		(apply #'plan-property alias property))
	    properties)
    (format *from-clause* "  FROM ~a AS ~a~%" table-name alias)
    (apply #'plan-superclass-mappings
	   alias superclass-mappings)
    (apply #'plan-subclass-mappings
	   (apply #'append-alias alias primary-key)
	   subclass-mappings)))

(defvar *select-list*)
(defvar *from-clause*)

(defun make-join-plan (class-mapping)
  (let ((*select-list* (make-string-output-stream))
	(*from-clause* (make-string-output-stream)))
    (plan-class-mapping (make-alias) class-mapping)
    (write-string (get-output-stream-string *select-list*))
    (write-string (get-output-stream-string *from-clause*))))

;;(defun fetch (root reference &rest references))

;;(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &key (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (assoc class-name mapping-schema :key #'first))
	 (*table-index* 0))
    (make-join-plan class-mapping)))


(define-parser *expression-parser*
    (:start-symbol expression)
  (:terminals (class-name table-name))
  (:precedence ())
  (class-mapping class-name
		 table-name
		 primary-key
		 superclass-mappings
		 subclass-mappings
		 slot-mappings)
  (class-name class-name)
  (table-name table-name)
  (primary-key column-names)
  (superclass-mappings
   (superclass-mapping superclass-mappings)
   nil)
  (superclass-name table-name
		   primary-key
		   superclass-mappings
		   slot-mappings
		   subclass-mappings)
  (subclass-mappings
   (subclass-mapping subclass-mappings)
   nil)
  (subclass-name class-name
		 table-name
		 primary-key
		 superclass-mappings
		 slot-mappings
		 subclass-mappings)
  (slot-mappings
   (slot-mapping slot-mappings)
   nil)
  (slot-mapping
   (property columns)
   (many-to-one class-mapping foreign-key)
   (one-to-many class-mapping foreign-key))
  (foreign-key column-names)
  (column-names
   (column-name column-names)
   column-name))
