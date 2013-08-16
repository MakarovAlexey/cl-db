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

(defclass table-reference ()
  ((table :initarg :table :reader table-of)
   (joins :initarg :joins :reader joins-of)
   (references :initarg :references :reader references-of)
   (slot-accesses :initarg :slot-accesses :reader slot-accesses-of)
   (expressions :initarg :expressions :reader expressions-of)))

(defclass join (table-reference)
  ((join-columns :initarg :on :reader join-columns-of)))

(defclass inner-join (join)
  ())

(defclass left-join (join)
  ())

Собираем информацию о данных участвующих в запросе. Строим query-info.

Затем создаем структуру запроса отражая связи между таблицами
(table-reference). Попутно указываем ссылки и выражения на основе
которых создана связь.

Анализ связей для загрузочников объектов.
 
В таком случае, при загрузке ассоциаций вместе с объектами некоторой
иерархии. Загрузка ассоциации будет проводиться по свом объекдинениям
таблиц, а сам класс по своим.

Необходимо реализовать возможность использования таблиц иерархии
наследования для обращений к значениям слотов и ассоциациям.

Таким образом в запросах не будет избыточного количества объединений
таблиц.

Поэтому, query-node суперкласс иерархии подклассами которой будет
структура запроса в каноническом виде - от корня к листям
(в противоположность переданных в запрос выражения в обычном виде, от
листьев к корням). Данный граф можно будет использовать для генерации 
всех частей запроса (FROM, WHERE, ORDER BY, HAVING) и для загрузки
результатов запроса (select list loaders).

Подклассы: object-loader, value-access-loader,
expression-result-loader.

NB: данные подклассы используются только для отметки мест загрузки
результата (select list).

****

Создаем query-loader. Здесь, root-bindings и refrence-bindings,
как связующие звенья, снимаются и предстают в виде отношений таблиц.
Ссылки на них могут остаться только, как резултат. Здесь отношения
таблиц можно переводить в SQL как выражение "FROM". Осталось создать
загрузочники для select-list.

Для этого необходимо собрать загружаему информацию по дереву.
Делается это обходом дерева до нижнего уровня.
В ходе этого необходимо собрать информацию о таблицах ... какую информацию и в какой форме?

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

(defclass query-info ()
  ((roots :initarg :roots :accessor roots-of)
   (references :initarg :references :accessor references-of)
   (expressions :initarg :expressions :accessor expressions-of)
   (values-access :initarg :values-access :accessor values-access-of)))

(defun accumulate (query-info &key root reference expression value-access)
  (with-accessors ((roots roots-of)
		   (references references-of)
		   (expression expressions-of)
		   (values-access values-access-of))
      query-info
    (make-instance 'query-info
		   :roots (list* root roots)
		   :values-access (list* value-access values-access)
		   :references (list* reference references)
		   :expression (list* expression expressions))))

(defun append-accumulated (&rest query-info)
  (make-instance 'query-info
		 :roots (reduce #'append query-info
				:key #'roots-of)
		 :references (reduce #'append query-info
				     :key #'references-of)
		 :expressions (reduce #'append query-info
				      :key #'expressions-of)
		 :values-access (reduce #'append query-info
					:key #'values-access-of)))

(defgeneric visit (instance visitor))

(defmethod visit ((instance root-binding)
		  &optional (visitor (make-query-info)))
  (accumulate visitor :root instance))

(defmethod visit ((instance reference-binding)
		  &optional (visitor (make-query-info)))
  (visit (parent-binding-of instance)
	 (accumulate visitor :reference instance)))

(defmethod visit ((instance value-access)
		  &optional (visitor (make-query-info)))
  (visit (parent-binding-of value)
	 (accumulate visitor :value-access instance)))

(defmethod visit ((instance expression)
		  &optional (visitor (make-query-info)))
  (apply #'append-accumulated
	 (accumulate visitor :expression instance)
	 (mapcar #'visit (arguments-of expression))))

(defun make-query-info (select-list where-clause
			order-by-clause having-clause)
  (apply #'append-accumulated
	 (append
	  (mapcar #'visit select-items)
	  (mapcar #'visit where-clause)
	  (mapcar #'visit order-by-clause)
	  (mapcar #'visit having-clause))))

(defun make-query-path (select-list where-clause
			order-by-clause having-clause)
  (let ((root-bindings
	 (list-root-bindings select-list where-clause
			     order-by-clause having-clause))
	(reference-bindings
	 (list-reference-bindings select-list where-clause
				  order-by-clause having-clause))
	(slots-access
	 (list-slots-access select-list where-clause
			    order-by-clause having-clause))
	(expressions
	 (list-expressions select-list where-clause
			   order-by-clause having-clause)))
    (mapcar #'(lambda (root-binding)
		(make-query-path-node root-binding
				      reference-bindings
				      slots-access
				      expressions))
	    root-bindings)))

(defun make-query-mapping (binding &rest reference-bindings)
  (make-instance 'query-mapping :binding binding
		 :reference-mappings 
		 (mapcar #'(lambda (binding)
			     (apply #'make-query-mapping
				    binding
				    reference-bindings))
			 (apply #'find-reference-bindings
				binding reference-bindings))))

;; 1) полиморфные запросы - вычисление таблиц и связей до подклассов
;;    если такоевые указаны.
;; 2) вычисление таблиц и связей необходимых только для загрузки 
;;    указанного в спеске выбора



;; Для анализа таблиц и связей необходимо локализовать оборащения из
;; выражений из select-list where having order-by в
;; дерево отображения root-mappings

;; Это даст возможность определить таблицы и их связи для формирования FROM-выражения

; аналог FROM содержится в select-list как корневые объекты для запроса
;; локализовать условия из  в дерево?
	 
(defun make-query (select-list &key where order-by having
		   limit offset singlep)
  (let ((root-mappings
	 (make-root-mappings select-list where order-by having)))
    (make-instance 'db-query
		   :root-mappings root-mappings
		   :select-items (mapcar #'(lambda (select-item)
					     (apply #'make-select-item
						    select-item
						    root-mappings))
					select-list)
		   :where
		   :having
		   :order-by


		   
		    
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



