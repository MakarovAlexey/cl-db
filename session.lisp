(in-package #:cl-db)

(defvar *default-database-interface*)

(defvar *default-connection-args*)

(defvar *default-mapping-schema*)

(defvar *session*)

(defclass clos-session ()
  ((database-interface :initarg :database-interface
		       :reader database-interface-of)
   (mapping-schema :initarg :mapping-schema
		   :reader mapping-schema-of)
   (connection :initarg :connection
	       :reader connection-of)
   (loaded-objects :initarg :loaded-objects
		   :reader loaded-objects-of)))

(defun open-session (mapping-schema
		     database-interface &rest connection-args)
  (make-instance 'clos-session
		 :mapping-schema mapping-schema
		 :databse-interface database-interface
		 :connection (apply #'open-connection
				    database-interface
				    connection-args)))

(defun close-session (session)
  (close-connection session))

(defun call-with-session (function database-interface-name
			  mapping-schema-name &rest connection-args)
  (let* ((*session*
	  (apply #'open-session
		 (ensure-mapping-schema mapping-schema-name)
		 (get-database-interface database-interface-name)
		 connection-args)))
    (funcall function)
    (close-session *session*)))

(defmacro with-session ((&rest options) &body body)
  `(apply #'call-with-session
	  #'(lambda () ,@body)
	  (quote ,(or (first
		       (compile-option :database-interface options))
		      *default-database-interface*))
	  (quote ,(or (first
		       (compile-option :mapping-schema options))
		      *default-mapping-schema*))
	  ,(or (compile-option :connection-args options)
	       *default-connection-args*)))

;; 1. надо проверять открытие и закрытие соединения

;; 2. продумать: сессия только открывает соединение и собирает объекты
;; транзакции (единицы работы (unit of work)) являются контекстом
;; работы с объектами т.е. вне транзации не производится отслеживания
;; состояния объектов, но можно их загружать (stateless session в
;; nhibernate)

