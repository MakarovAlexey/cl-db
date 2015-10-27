(in-package :cl-db)

(defvar *query-output*)

(defun list-previous-contexts (context)
  (let ((previous-context
	 (previous-context-of context)))
    (when (not (null previous-context))
      (list* previous-context
	     (list-previous-contexts context)))))

(defun write-sql-query (stream context)
  (format stream "SELECT ~{~/write-select-item~^, /~}
                    FROM ~{~{~/write-table-reference/~^~%~}~^~%  JOIN ~}"))

(defun write-query (stream context)
  (let ((previous-contexts (list-previous-contexts context)))
    (when (not (null previous-contexts))
      (write-auxlilliary-statements stream previous-contexts))
    (write-sql-query context)))

(defun compile-query (context)
  (with-open-stream (query-stream (make-string-output-stream))
    (write-query query-stream context)
    (get-output-stream-string *query-output*)))

    
    
