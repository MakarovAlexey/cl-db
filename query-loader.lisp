(in-package #:cl-db)

;; setf fetch to query-root, joined-reference, subclass-root and fetch-reference

(defun load-properties (object class-maping root context row)
  (let ((class))))




		     (get-root-node class-node)
		     
    (dolist (property-mapping (property-mappings-of class-mapping))
      (setf (slot-value class-node (slot-name-of property-mapping))
	    (get-column-value (column-of property-mapping)
			      class-node
			      context)))
    (dolist (superclass-mapping (superclass-mappings-of class-mapping))
      (load-object
       (get-class-mapping
	(referenced-class-of superclass-mapping)) root context row))
    object))
    

 context row)))


(defun load-object (object class-mapping root context row)
  (load-properties
   (or
    (load-subclass-object root context row)
    (allocate-instance
     (class-name-of class-mapping))) class-mapping root context row))

(defun load-object (primary-key class-mapping root context row session)
  (let ((class-mapping (class-mapping-of root))
	(root-node (get-root-node root)))
    (multiple-value-bind (object presentp)
	(gethash (load-primary-key class-mapping
      (primary-key-of class-mapping) root-node context)
     (ensure-gethash class-mapping
		     (loaded-objects-of session)
		     (make-hash-table :test equal))
     (load-object class-mapping root-node context row))))

(defgeneric load-select-item (select-item row context session))

(defmethod load-select-item ((select-item query-root) row column-map session)
  (let ((primary-key (load-primary-key select-item context row)))
    (multiple-value-bind (object presentp)
	(gethash primary-key (ensure-gethash
			      (class-mapping-of root)
			      (loaded-objects-of session)
			      (make-hash-table :test equal)))
      (if (not presentp)
	  (load-object primary-key (allocate-instance select-item context row session)
	  object))))

(defun load-row (row selec-list column-map session)
  (mapcar #'(lambda (select-item)
	      (load select-item row column-map session))
	  rows))

(defun make-class-node-column-map (column-names columns)
  (let ((column-map (make-hash-table)))
    (dolist (column columns column-map)
      (let ((column (rest column)))
	(setf (gethash (name-of column) column-map)
	      (position (format nil "~a.~a"
				(name-of column)
				(name-of
				 (correlation-of column)))
			column-names))))))

(defun make-column-map (column-names context)
  (let ((column-map (make-hash-table)))
    (maphash #'(lambda (class-node columns)
		 (setf (gethash class-node column-map)
		       (make-class-node-column-map column-names columns)))
	     (class-node-columns-of context))    
		   

;; result row + column-map = row

(defun load-all (rows context select-list session)
  (let ((column-map (make-column-map (first rows) context)))
    (mapcar #'(lambda (row)
		(load-row row column-map select-list session))
	  (rest rows))))
