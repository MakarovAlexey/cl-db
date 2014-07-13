(in-package #:cl-db)

(defvar *table-index*)

(defun make-alias (&optional (prefix "table"))
  (format nil "~a_~a" prefix (incf *table-index*)))

(defclass query-class (class-mapping)
  ((alias :initarg :alias))

(defclass join-superclass (inheritance-mapping)
  ((class-table-name :initarg :class-table-name)
   (class-table-alias :initarg :class-table-alias)
   (superclass-table-name :initarg :superclass-table-name)
   (superclass-table-alias :initarg :superclass-table-alias)
   (superclass-primary-key :initarg :superclass-primary-key)))

(defclass join-subclass (inheritance-mapping)
  ((class-table-alias :initarg :class-table-alias)
   (class-primary-key :initarg :class-primary-key)
   (subclass-table-name :initarg :subclass-table-name)
   (subclass-table-alias :initarg :subclass-table-alias)))

(defrule plan-class-superclass ()
  (query-class (class-name ?class-name))
  (class-mapping (class-name ?class-name)
		 (table-name ?table-name)
		 (primary-key ?primary-key))
  (inheritance-mapping (class-name ?class-name)
		       (superclass-name ?superclass-name)
		       (foreign-key ?foreign-key))
  (superclass-mapping (class-name ?superclass-name)
		      (table-name ?superclass-table-name)
		      (primary-key ?superclass-primary-key))
  =>
  (assert-instnce
   (make-instance 'join-superclass
		  :class-name ?class-name
		  :superclass-name ?superclass-name
		  :class-table-name ?table-name
;;		  :class-table-alias 
		  :superclass-table-name ?superclass-table-name
		  :superclass-primary-key ?superclass-primary-key
;;		  :superclass-table-alias
		  :foreign-key ?foreign-key)))

(defrule plan-superclass-superclass ()
  (join-superclass (superclass-name ?class-name))
  (class-mapping (class-name ?class-name)
		 (table-name ?table-name)
		 (primary-key ?primary-key))
  (inheritance-mapping (class-name ?class-name)
		       (superclass-name ?superclass-name)
		       (foreign-key ?foreign-key))
  (superclass-mapping (class-name ?superclass-name)
		      (table-name ?superclass-table-name)
		      (primary-key ?superclass-primary-key))
  =>
  (assert-instnce
   (make-instance 'join-superclass
		  :class-name ?class-name
		  :superclass-name ?superclass-name
		  :class-table-name ?table-name
;;		  :class-table-alias 
		  :superclass-table-name ?superclass-table-name
		  :superclass-primary-key ?superclass-primary-key
;;		  :superclass-table-alias
		  :foreign-key ?foreign-key)))

(defrule plan-class-subclass ()
  (query-class (class-name ?class-name))
  (class-mapping (class-name ?class-name)
		 (table-name ?table-name)
		 (primary-key ?primary-key))
  (inheritance-mapping (subclass-name ?subclass-name)
		       (superclass-name ?class-name)
		       (foreign-key ?foreign-key))
  (subclass-mapping (class-name ?subclass-name)
		    (table-name ?subclass-table-name)
		    (primary-key ?subclass-primary-key))
  =>
  (assert-instnce
   (make-instance 'join-subclass
		  :class-name ?class-name
		  :subclass-name ?subclass-name
		  :class-table-name ?table-name
;;		  :class-table-alias 
		  :subclass-table-name ?subclass-table-name
		  :subclass-primary-key ?subclass-primary-key
;;		  :subclass-table-alias
		  :foreign-key ?foreign-key)))

(defrule plan-subclass-subclass ()
  (join-subclass (subclass-name ?class-name))
  (class-mapping (class-name ?class-name)
		 (table-name ?table-name)
		 (primary-key ?primary-key))
  (inheritance-mapping (subclass-name ?subclass-name)
		       (superclass-name ?class-name)
		       (foreign-key ?foreign-key))
  (subclass-mapping (class-name ?subclass-name)
		    (table-name ?subclass-table-name)
		    (primary-key ?subclass-primary-key))
  =>
  (assert-instnce
   (make-instance 'join-subclass
		  :class-name ?class-name
		  :subclass-name ?subclass-name
		  :class-table-name ?table-name
;;		  :class-table-alias 
		  :subclass-table-name ?subclass-table-name
		  :subclass-primary-key ?subclass-primary-key
;;		  :subclass-table-alias
		  :foreign-key ?foreign-key)))

(defun merge-trees (&rest trees)
  (reduce #'(lambda (result tree)
	      (list*
	       (list*
		(first tree)
		(apply #'merge-trees
		       (append
			(rest tree)
			(rest
			 (assoc
			  (first tree) result)))))
	       (remove (first tree) result :key #'first)))
	  trees :initial-value nil))



(defun plan-inheritance (&rest inheritance-mappings)
  (mapcar #'(lambda (inheritance-mapping)
	      (list*
	       (superclass-mapping-of inheritance-mapping)
	       (make-alias (incf *table-index*))
	       (columns-of inheritance-mapping)
	       (apply  #'plan-inheritance
		       (superclass-mapping-of inheritance-mapping))))
	  inheritance-mappings))

(defun plan-extension (class-mapping &optional root-superclass)
  (mapcar #'(lambda (extension-mapping)
	      (list*
	       (list*
		(subclass-mapping-of extension-mapping)
		(make-alias (incf *table-index*))
		(columns-of extension-mapping)
		(apply #'plan-inheritance
		       (remove root-superclass
			       (inheritance-mappings-of
				(subclass-mapping-of extension-mapping)))))
	       (plan-extension
		(subclass-mapping-of extension-mapping) class-mapping)))
	  (extension-mappings-of class-mapping)))

(defun make-join-plan (class-mapping)
  (list*
   (list* class-mapping
	  (make-alias (incf *table-index*))
	  (apply #'plan-inheritance
		 (inheritance-mappings-of class-mapping)))
   (plan-extension class-mapping)))

(defun make-loaders (class-mapping object-plan)
  (acons class-mapping object-plan
	 (reduce #'append
		 (extension-mappings-of class-mapping)
		 :key #'(lambda (extension-mapping)
			  (make-loaders
			   (subclass-mapping-of extension-mapping)
			   (assoc extension-mapping (rest object-plan)))))))

(defun print-extension (class-mapping alias columns &rest superclasses)
  (list :class-mapping class-mapping
	:alias alias
	:columns columns
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'print-inheritance superclass))
			      superclasses)))

(defun print-extensions (extension &rest extensions)
  (list :extension (apply #'print-extension extension)
	:extensions (mapcar #'(lambda (extension)
				(apply #'print-extensions extension))
			    extensions)))

(defun print-inheritance (class-mapping columns &rest superclasses)
  (list :class-mapping class-mapping
	:alias (sxhash class-mapping)
	:columns columns
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'print-inheritance superclass))
			      superclasses)))

(defun print-root (class-mapping &rest superclasses)
  (list :class-mapping class-mapping
	:alias (sxhash class-mapping)
	:superclasses (mapcar #'(lambda (superclass)
				  (apply #'print-inheritance superclass))
			      superclasses)))

(defun print-from-clause (alias root &rest extensions)
  (list :root root
	:alias alias
	:extension extensions))

(defun fetch (root reference &rest references))

(defun join (class-names root reference &key (join #'skip) where order-by having))

(defun db-read (class-name &optional (mapping-schema *mapping-schema*))
  (let* ((class-mapping
	  (assoc class-name (first mapping-schema)))
	 (*table-index* 0))
    (apply #'print-from-clause class-mapping)))
