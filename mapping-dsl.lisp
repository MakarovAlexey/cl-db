;; implement (or class-name null) - type form
;; implement type declaration for values

(in-package #:cl-db)

(defvar *mapping-prefix* "-MAPPING")

(defclass configuration ()
  ((open-connection-fn :initarg :open-connection-fn
		       :reader open-connection-fn-of)
   (close-conection-fn :initarg :close-conection-fn
		       :reader close-conection-fn-of)
   (prepare-statement-fn :initarg :prepare-statement-fn 
			 :reader prepare-statement-fn-of)
   (execute-statement :initarg :execute-statement
		      :reader execute-statement-fn)
   (mapping-schema :initarg :mapping-schema
		   :reader mapping-schema-of)))

(defun assert-duplicate-slot-mappings (slot-mappings)
  (reduce #'(lambda (slot-names slot-name)
	      (if (find slot-name slot-names)
		  (error "Duplicate slot mapping: ~a" slot-name)
		  (list* slot-names slot-name)))
	  slot-mappings
	  :key #'first))

(defun compile-option (option &rest options)
  (rest (assoc option options)))

(defun make-value-mapping (slot-name marshaller unmarshaller
			   column &rest columns)
  `(make-instance 'value-mapping-definition
		  :slot-name (quote ,slot-name)
		  :marshaller ,marshaller
		  :unmarshaller ,unmarshaller
		  :columns (list
			    ,@(loop for (name sql-type)
				 in (list* column columns)
				 collect `(make-instance 'column-definition
							 :column-name ,name
							 :column-type ,sql-type)))))

(defun make-many-to-one-mapping (slot-name marshaller unmarshaller
				 mapped-class column &rest columns)
  `(make-instance 'many-to-one-mapping-definition
		  :slot-name (quote ,slot-name)
		  :referenced-class (find-class (quote ,mapped-class))
		  :columns-names (quote ,(list* column columns))
		  :marshaller ,marshaller
		  :unmarshaller ,unmarshaller))

(defun make-one-to-many-mapping (slot-name marshaller unmarshaller
				 mapped-class column &rest columns)
  `(make-instance 'one-to-many-mapping-definition
		  :slot-name (quote ,slot-name)
		  :referenced-class (find-class (quote ,mapped-class))
		  :columns-names (quote ,(list* column columns))
		  :marshaller ,marshaller
		  :unmarshaller ,unmarshaller))

(defun compile-slot-mappings (type function &rest slot-mappings)
  (let ((mappings (list)))
    (dolist (slot-mapping slot-mappings)
      (destructuring-bind
	    (slot-name (mapping-type &rest options)
		       &optional marshaller unmarshaller)
	  slot-mapping
	(when (eq mapping-type type)
	  (push (apply function slot-name
		       marshaller unmarshaller options)
		mappings))))
    (reverse mappings)))

(defmacro define-class-mapping ((class-name table-name)
				options	&body slot-mappings)
;;  (assert-duplicate-slot-mappings slot-mappings)
  (values
   `(defun ,(alexandria:symbolicate class-name *mapping-prefix*) ()
      (make-instance 'class-mapping-definition
		     :mapped-class (find-class (quote ,class-name))
		     :table-name ,table-name
		     :primary-key (quote ,(apply #'compile-option
						:primary-key
						options))
		     :superclasses (mapcar #'find-class
					   (quote
					    ,(apply #'compile-option
						    :superclasses
						    options)))
		     :value-mappings
		     (list ,@(apply #'compile-slot-mappings :value
				    #'make-value-mapping
				    slot-mappings))
		     :many-to-one-mappings
		     (list ,@(apply #'compile-slot-mappings :many-to-one
				    #'make-many-to-one-mapping
				    slot-mappings))
		     :one-to-many-mappings 
		     (list ,@(apply #'compile-slot-mappings :one-to-many
				    #'make-one-to-many-mapping
				    slot-mappings))))))
;;   (ensure-configuration-schema)))

(defmacro define-configuration ((&key package *package*)
				&body database-access-functions)
    `(defparameter
	 ,(alexandria:ensure-symbol *configuration-variabe-name*)
       (make-instance 'configuration
		      :open-connection-fn ,(apply #'compile-option
						  :open-connection options)
		      :close-connection-fn ,(apply #'compile-option
						   :close-connection options)
		      :prepare-statement-fn ,(apply #'compile-option
						    :prepare options)
		      :execute-statement-fn ,(apply #'compile-option
						    :execute options)
		      :mapping-schema ,(compile-mapping-schema))))

;; прикрутить компиляцию схемы
;; взаимоувязать переопределение или добавление нового отображения
;; с пересозданием схемы


;;(defvar *class-mapping-definitions* (make-hash-table))

;; при определеении отображения класса, в пакете создается функция
;; с именем вида {class-name}-mapping
;; далее при вызове with-session в указанном пакете (по умолчанию
;; в текущем) производится поиск всех {class-name}-mapping и создание
;; в нем же переменной с именем указанным в переменной (*mapping-schema-symbol* "*mapping-schema*" ???)

;; cl-db:default-mapping-schema - может ввести отображение по умолчанию?
;; в этом не нужды, так как отображение можно определить в отдельном
;; пакете. Если реализовать экспорт символов отображения
;; и имопртровать их в нужный пакет? (:use в defpackage или use-mapping)

;; возможно имеет смысл регистрировать определения по пакетам, но не
;; интернировать символы (учитыать по пакетам и названиям классов)

;; 1) импортируемость (возможность определения нескольких схем отображения)
;; 2) возможность определения схемы внутри пакета

;; define-class-mapping вычисляет переменную *mapping-schema*

;; ensure-symbol, (setf symbol-value)



(define-condition class-mapping-redefinition (style-warning)
  ((mapped-class :initarg :mapped-class :reader mapped-class)))



;;  (let ((mappings (loop for symbol being the symbols in package
;;		     when (search *mapping-prefix* (symbol-name symbol))
;;		     collect symbol)))



(defun sql-name (string)
  (substitute #\_ #\- (string-downcase string)))

(defun lisp-name (string)
  (substitute #\- #\_ (string-upcase string)))

