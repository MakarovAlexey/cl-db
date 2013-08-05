(in-package #:cl-db)

(defclass root-binding ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)))

(defclass reference-binding ()
  ((parent-binding :initarg :parent-binding
		   :reader parent-binding-of)
   (reference-mapping :initarg :reference-mapping
		      :reader reference-mapping-of)))

(defclass value ()
  ((mapping :initarg :mapping
	    :reader mapping-of)))

(defclass expression ()
  ((expression-type :initarg :expression-type
		    :reader expression-type-of)
   (arguments :initarg :arguments
	      :reader arguments-of)))

(defclass root-binding-mapping ()
  )

(defclass reference-binding-mapping ()
  )

(defun bind-root (class-name &optional
		  (mapping-schema *mapping-schema*))
  (make-instance 'root-binding
		 :class-mapping (get-class-mapping
				 (find-class class-name)
				 mapping-schema)))

(defun bind-reference (parent-binding accessor)
  (make-instance 'reference-binding
		 :parent-binding parent-binding
		 :reference-mapping (get-reference-mapping
				     (class-mapping-of parent-binding)
				     accessor)))

(defgeneric append-root-bindings (root-bindings select-items))

(defmethod append-root-bindings (root-bindings (select-items list))
  (reduce #'append-root-bindings select-items
	  :initial-value root-bindings))

(defmethod append-root-bindings (root-bindings (binding root-binding))
  (list* binding root-bindings))

(defmethod append-root-bindings (root-bindings (binding reference-binding))
  (append-root-bindings root-bindings (parent-binding-of binding)))

(defmethod append-root-bindings (root-bindings (value value))
  (append-root-bindings root-bindings (parent-binding-of value)))

(defmethod append-root-bindings (root-bindings (expression expression))
  (append-root-bindings root-bindings (arguments-of expression)))

(defun list-root-bindings (&rest select-items)
  (remove-duplicates
   (append-root-bindings (list) select-items)))

(defgeneric append-reference-bindings (reference-bindings select-items))

(defmethod append-reference-bindings (reference-bindings
				      (select-items list))
  (reduce #'append-reference-bindings select-items
	  :initial-value reference-bindings))

(defmethod append-reference-bindings (reference-bindings
				      (binding root-binding))
  nil)

(defmethod append-reference-bindings (reference-bindings
				      (binding reference-binding))
  (list* binding reference-bindings))

(defmethod append-reference-bindings (reference-bindings (value value))
  (append-reference-bindings reference-bindings
			     (parent-binding-of value)))

(defmethod append-reference-bindings (reference-bindings
				      (expression expression))
  (append-reference-bindings reference-bindings
			     (arguments-of expression)))

(defun list-reference-bindings (&rest select-items)
  (remove-duplicates
   (append-reference-bindings (list) select-items)))

(defun find-reference-bindings (binding &rest reference-bindings)
  (remove binding reference-bindings
	  :key #'parent-binding-of :test-not #'eq))

(defun make-query-mapping (binding &rest reference-bindings)
  (make-instance 'query-mapping :binding binding
		 :reference-mappings 
		 (mapcar #'(lambda (binding)
			     (apply #'make-query-mapping
				    binding
				    reference-bindings))
			 (apply #'find-reference-bindings
				binding reference-bindings))))

(defun make-query (select-list)
  (let* ((root-bindings (apply #'list-root-bindings select-items))
	 (reference-bindings (apply #'list-reference-bindings
				    select-items))
	 (query-mappings (mapcar #'(lambda (root-binding)
				     (apply #'make-query-mapping
					    root-binding
					    reference-bindings))
				 root-bindings))
	 (
    
    ( (make-loaders e-loader select-list)))
    (

=> make-loaders

(defun sql-query (query)
  ("SELECT"
   (select-list-of query)
   "FROM"
   (tquery-mappings-of query)


;; у любого выражения (expression) загружается только результат - значение
;; у любого связывания (binding), объхекта или ассоциций загружается объект

;; Результат запроса обрабатывается двумя загрузчиками:
;; 1. объектный загрузчик (object loader).
;; Нужны все данные по объектам из БД
;; ассциации загружаются также, объектным загрузчиком
;; 2. загрузчик результата выражения (expression loader)
;; Нужна только часть, участвующая в выражении.
;; Надо собрать информацию о привязках используемых 
;; для в запросе.

;; когда надо назначать псевдонимы таблицам? Ведь они важны для загрузки.

;; Для загрузки объекта - создется загрузочник объекта в котором вся эта
;; информация есть. Для загрузки значения выражения требуются только
;; связи между таблицами.

;; Какую информацию необходимо сообщить загрузчику значения?

;; Сперва создаем отображение запроса (query-mappings).
;; Результатом будет список объектов query-mapping. Они понадобятся для
;; построения загрузочников. В отображении участвуют связывания.

(defclass expression-loader ()
  ((alias :initarg :alias :reader alias-of)
   (tables :initarg :tables :reader tables-of)
   (...)))

(defclass joined-superclass ()
  ((class-mapping :initarg :class-mapping
		  :reader class-mapping-of)
   (table-reference :initarg :table-reference
		    :reader table-reference-of)
   (joined-superclasses :initarg :joined-superclasses
			:reader joined-superclasses-of)
   (joined-fetchings :initarg :joined-fetchings
		     :reader joined-fetchings-of)))

(defclass object-loader (joined-superclass)
  ((subclass-object-loaders :initarg :subclass-object-loaders
			    :reader subclass-object-loaders-of)))

(defun compute-joined-superclasses (superclasses-mappings)
  (mapcar #'(lambda (superclass-mapping)
	      (make-instance 'joined-superclass
			     :class-mapping (class-mapping-of superclass-mapping)))
	  superclasses-mappings))

(defmethod initialize-instance :after ((instance joined-superclass)
				       &key class-mapping
				       (joined-superclasses
					(compute-joined-superclasses
					 (superclasses-mappings-of class-mapping))))
  (setf (slot-value instance 'joined-superclasses) joined-superclasses))

(defun compute-subclass-object-loaders (object-loader class-mapping)
  (mapcar #'(lambda (subclass-mapping)
	      (make-instance 'object-loader
			     :class-mapping subclass-mapping
			     :joined-superclasses (list* object-loader
							 (compute-joined-superclasses
							  (remove class-mapping
								  (superclasses-mappings-of subclass-mapping))))))
	  (subclasses-mappings-of class-mapping)))

(defmethod initialize-instance :after ((instance object-loader)
				       &key class-mapping
				       fetched-associations)
  (setf (slot-value instance 'subclass-object-loaders)
	(compute-subclass-object-loaders instance class-mapping)))

(defgeneric make-loader (select-item))

(defmethod make-loader ((binding root-binding))
  (make-instance 'root-loader
		 :class-mapping (class-mapping-of binding)))

(defmethod make-loader ((binding reference-binding))
  (make-instance 'reference-loader
		 :reference-binding binding
		 :parent-loader (ensure-loader (parent-






	     
  


    





(defvar *expression-types* (make-hash-table))

(defun register-expression (function name &rest names)
  (dolist (name (list* name names) function)
    (setf (gethash name *expressions*) function)))



;; connection name parameters


;; object-loader
;; table-loader - загрузка части объекта?
;; root-table-alias
;; join-table-alias - возможно inner-join-table???

;;(defun make-loader (select-list fetched-associations singlep)
;;  (declare (ignore joined-fetchings))
;;  (make-instance 'object-loader
;;		 :class-mapping class-mapping
;;		 :joined-superclasses (compute-joined-superclasses
;;				       (superclasses-mappings-of class-mapping))))

;;(defun make-sql (select-list fetched-associations
;;		 where having order-by limit offset)
;;  (format nil "SELECT ~a FROM ~a"
;;	  (make-select-list select-list)
;;	  (make-from-clause select-list)
;;	  (make-where-clause where
;; нужен loader из-за псевдонимов таблиц и столбцов

;;(defun db-list (select-list &key fetch singlep
;;		where having order-by limit offset)
;;  (let ((loader (make-loader select-list fetch singlep)))
;;    (apply #'db-load loader
;;	   (apply #'execute
;;		  (ensure-prepared
;;		   (make-sql select-list fetch where
;;			     having order-by limit offset))
;;		  (get-parameters where having limit offset)))))

;;select-list:
;; objects
;; values
;; operators
;; function calls
;; aggregate function calls

;; symbol - alias of from clause
;; sexp
;;(defun compile-expression (expression)

;;(defun compile-select-list (&rest expressions)
;;  (mapcar #'compile-expression expressions))

;; желательно реализовать компиляцию запросов с сохранением результатов
;; в конфигурации сессии

;; каждый запрос сопоставлен с символом
;; (как функция принимающая подключение и возвращающая результат запроса)
;; 1. синтакический разбор запроса
;; 2. компиляция вместе с OR-отображением через вызов (compile-mappings)

(defgeneric compile-binding (name binding))

(defmethod compile-binding (name (binding symbol))
  `(,name (make-loader (quote binding))))

(defmethod compile-binding (name (binding list))
  (destructuring-bind (association-accessor binding-name) binding
    `(,name
      (join-association binding-name
			(get-association-name association-accessor)))))

(defun compile-bindings (&rest bindings)
  (mapcar #'(lambda (binding)
	      (apply #'compile-binding binding))
	  bindings))

(defmacro db-query (bindings &optional clauses)
  `(let* (,@(apply #'compile-bindings bindings))
     (db-list ,@(or (apply #'compile-select-list
			   (apply #'get-option :select clauses))
		    (apply #'compile-default-select-list bindings))
	      :limit (apply #'get-option :limit clauses)
	      :offset (apply #'get-option :offset clauses)
	      :singlep (apply #'get-option :single clauses)
	      :fetch ,@(apply #'compile-fetch-clause
			      (apply #'get-option :fetch clauses))
	      :where ,@(apply #'compile-where-clause
			      (apply #'get-option :where clauses))
	      :having ,@(apply #'compile-having-clause
			       (apply #'get-option :having clauses))
	      :order-by ,@(apply #'compile-order-by-clause
				 (apply #'get-option
					:order-by clauses)))))

;; (db-persist object)
;; (db-remove object)



