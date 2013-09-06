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

(defclass query-info ()
  ((root-bindings :initarg :root-bindings
		  :accessor root-bindings-of)
   (reference-bindings :initarg :reference-bindings
		       :accessor reference-bindings-of)
   (values-access :initarg :values-access
		  :accessor values-access-of)
   (expressions :initarg :expressions
		:accessor expressions-of)))

(defclass query-node ()
  ((superclass-inheritance-nodes :initarg :superclass-inheritance-nodes
				 :reader superclass-inheritance-nodes-of)
   (subclass-extension-nodes :initarg :subclass-extension-nodes
			     :reader subclass-extension-nodes-of)
   (reference-nodes :initarg :reference-nodes
		    :reader reference-nodes-of)
   (values-access :initarg :values-access
		  :reader values-access-of)))

(defclass query-binding-node (query-node)
  ((query-binding :initarg :query-binding
		  :reader query-binding-of)))

(defclass inheritance-node (query-node)
  ((inheritance-mapping :initarg :inheritance-mapping
			:reader inheritance-mapping-of)))

;;(defclass select-item ())

;;(defclass where-clause ())

;;(defclass order-by-clause ())

;;(defclass having-clause ())

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

(defun accumulate (query-info &key roots references
		   expressions values-access)
  (make-instance 'query-info
		 :values-access
		 (append (values-access-of query-info) values-access)
		 :expressions
		 (append (expressions-of query-info) expressions)
		 :references
		 (append (references-of query-info) references)
		 :roots
		 (append (roots-of query-info) roots)))

(defun append-accumulated (query-info &rest query-infos)
  (reduce #'(lambda (query-info query-info-2)
	      (accumulate query-info 
			  :roots (roots-of query-info-2)
			  :references (references-of query-info-2)
			  :expressions (expressions-of query-info-2)
			  :values-access (values-access-of query-info-2)))
	  query-info :initial-value query-info))

(defgeneric visit (instance visitor))

(defmethod visit ((instance root-binding)
		  &optional (visitor (make-instance 'query-info)))
  (accumulate visitor :roots (list instance)))

(defmethod visit ((instance reference-binding)
		  &optional (visitor (make-instance 'query-info)))
  (visit (parent-binding-of instance)
	 (accumulate visitor :references (list instance))))

(defmethod visit ((instance value-access)
		  &optional (visitor (make-instance 'query-info)))
  (visit (parent-binding-of value)
	 (accumulate visitor :values-access (list instance))))

(defmethod visit ((instance expression)
		  &optional (visitor (make-instance 'query-info)))
  (apply #'append-accumulated
	 (accumulate visitor :expressions (list instance))
	 (mapcar #'visit (arguments-of expression))))

(defun make-query-info (select-list where-clause
			order-by-clause having-clause)
  (let ((query-info (apply #'append-accumulated
			   (append
			    (mapcar #'visit select-items)
			    (mapcar #'visit where-clause)
			    (mapcar #'visit order-by-clause)
			    (mapcar #'visit having-clause)))))
    (setf (slot-value query-info 'order-by-clause) order-by-clause
	  (slot-value query-info 'having-clause) having-clause
	  (slot-value query-info 'where-clause) where-clause
	  (slot-value query-info 'select-list) select-list)))

(defun get-reference-bindings (query-binding query-info)
  (remove query-binding
	  (reference-bindings-of query-info)
	  :key #'parent-binding-of
	  :test-not #'eq))

(defun get-values-access (query-binding query-info)
  (remove query-binding
	  (values-access-of query-info)
	  :key #'parent-binding-of
	  :test-not #'eq))

(defun get-direct-values-access (query-binding query-info path)
  (remove-if-not #'(lambda (value-access)
		     (equal
		      (path-of (value-mapping-of value-access))
		      path))
		 (get-values-access query-binding query-info)))

(defun get-direct-reference-bindings (parent-object query-info path)
  (remove-if-not #'(lambda (reference-binding)
		     (equal
		      (path-of (reference-mapping-of reference-binding))
		      path))
		 (get-reference-bindings query-binding query-info)))

(defun filter-inheritance-mappings (query-binding root-binding
				    inheritance-mappings
				    &optional path)
  (if (not (member query-binding (select-list-of query-info)))
      (remove-if-not
       #'(lambda (inheritance-mapping)
	   (let ((inheritance-path (list* inheritance-mapping path)))
	     (null
	      (and
	       (get-direct-values-access query-binding
					 inheritance-path)
	       (get-direct-reference-bindings query-binding
					      inheritance-path)))))
       inheritance-mappings)
      inheritance-mappings))

;; фильтровать суперклассы суперкласса тоже нужно
(defun make-superclass-node (parent-object query-info
			     &optional inheritance-mapping &rest path)
  (let ((mapping-path (list* inheritance-mapping path)))
    (make-instance 'query-inheritance-node
		   :inheritance-mapping inheritance-mapping
		   :inheritance-nodes
		   (mapcar #'(lambda (inheritance-mapping)
			       (apply #'make-superclass-node
				      parent-object
				      query-info
				      inheritance-mapping
				      mapping-path))
			   (superclass-inheritance-mappings-of
			    (subclass-mapping-of inheritance-mapping)))
		   :values-access-nodes
		   (mapcar #'make-reference-node
			   (get-direct-values-access parent-object
						     query-info
						     mapping-path))
		   :reference-binding-nodes
		   (mapcar #'make-reference-node
			   (get-direct-references parent-object
						  query-info
						  mapping-path)))))

(defun make-binding-node (query-binding query-info
			  class-mapping &optional path)
  (make-instance 'query-binding-node
		 :query-binding query-binding
		 :superclass-inheritance-nodes
		 (mapcar #'make-superclass-node
			 (filter-inheritance-mappings
			  query-binding query-info
			  (superclass-inheritance-mappings-of
			   class-mapping)))
		 :subclass-extension-nodes
		 (when (member query-binding
			       (select-list-of query-info))
		   (mapcar #'make-subclass-node
			   (subclasses-inheritance-mappings-of class-mapping)))
		 :reference-binding-nodes
		 (mapcar #'make-reference-node
			 (get-direct-references query-binding
						query-info
						class-mapping
						path))
		 :value-access-nodes
		 (get-direct-values-access query-binding
					   query-info
					   class-mapping
					   path)))

(defun make-root-node (root-binding query-info)
  (make-binding-node root-binding
		     query-info
		     (class-mapping-of query-binding)))

(defmethod get-superclass-inheritance-mappings
    ((query-binding reference-binding))
  (superclass-inheritance-mappings-of
   (referenced-class-mapping-of query-binding)))



(defun get-superclass-inheritance (query-binding query-info))
  

(defun get-subclass-inheritance (query-binding query-info)
  (when (not (null (member query-binding
			   (select-list-of query-info))))
    (get-subclass-inheritance-mappings query-binding query-info)))




		 

(defun make-subclass-node (parent-object query-info
			   ignored-superclass-mapping)
  (

  ;; заполнить все суперклассы кроме ignored-superclass-mapping
  )

(defun make-reference-node (parent-object query-info)
  ;; проверить наличиве в select-list, при наличии заполнить
  ;; superclasess и subclases
  )

(defun get-expressions (parent-object query-info)
  ;; участие в выражении непосредственно
  )

;; 1. Создаем query-tree - анализируем что нужно загрузить (какие
;; связи: ассоциации и наследование).

;; 2. Создаем table-trees - те же отношения (1), но как отношения
;; таблиц (со связями: left joins и inner joins).

;; 3. Создаем select-list - список выражений, но уже по table-tree
;; (загрузка результата связана с этим списком).

;; 4. Аналогичено с select-list создаем where-claue, order-by-clause,
;; having-clause.

;; добавить fetch-also





(defun make-query (select-list &key where order-by
		   having limit offset single)
  (let ((query-info
	 (make-query-info select-list where order-by having))
	(query-trees
	 (mapcar #'(lambda (root-binding)
		     (make-binding-node root-binding query-info))
		 (root-bindings-of query-info)))
	(table-tree
	 (mapcar #'make-table-tree query-trees)))
    (make-instance 'db-query
		   :sql-query (make-sql-query table-tree select-list
					      where order-by having
					      limit offset)
		   :parameters (make-parameters select-list where
						order-by having
						limit offset)
		   :loader (make-loader table-tree select-list
					sinlge))))



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



;; Собираем информацию о данных участвующих в запросе. Строим
;; query-info.

;; Затем создаем структуру запроса отражая связи между таблицами
;; (table-reference). Попутно указываем ссылки и выражения на основе
;; которых создана связь.

;; Анализ связей для загрузочников объектов.
 
;; В таком случае, при загрузке ассоциаций вместе с объектами
;; некоторой иерархии. Загрузка ассоциации будет проводиться по свом
;; объекдинениям таблиц, а сам класс по своим.

;; Необходимо реализовать возможность использования таблиц иерархии
;; наследования для обращений к значениям слотов и ассоциациям.

;; Таким образом в запросах не будет избыточного количества
;; объединений таблиц.

;; Поэтому, query-node суперкласс иерархии подклассами которой будет
;; структура запроса в каноническом виде - от корня к листям (в
;; противоположность переданных в запрос выражения в обычном виде, от
;; листьев к корням). Данный граф можно будет использовать для
;; генерации всех частей запроса (FROM, WHERE, ORDER BY, HAVING) и для
;; загрузки результатов запроса (select list loaders).

;; Подклассы: object-loader, value-access-loader,
;; expression-result-loader.

;; NB: данные подклассы используются только для отметки мест загрузки
;;результата (select list).

;; ****

;; Создаем query-loader. Здесь, root-bindings и refrence-bindings, как
;; связующие звенья, снимаются и предстают в виде отношений таблиц.
;; Ссылки на них могут остаться только, как резултат. Здесь отношения
;; таблиц можно переводить в SQL как выражение "FROM". Осталось
;; создать загрузочники для select-list.

;; Для этого необходимо собрать загружаему информацию по дереву.
;; Делается это обходом дерева до нижнего уровня. В ходе этого
;; необходимо собрать информацию о таблицах...



;;(defun make-sql-query (select-list where order-by having limit offset)
;;  (format "SELECT ~a FROM ~a ~@[WHERE ~a~] ~@[ORDER BY ~a~]









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
;;
;; 2) вычисление таблиц и связей необходимых только для загрузки
;;    указанного в спеске выбора

;; Для анализа таблиц и связей необходимо локализовать оборащения из
;; выражений из select-list where having order-by в дерево отображения
;; root-mappings

;; Это даст возможность определить таблицы и их связи для формирования
;; FROM-выражения

; аналог FROM содержится в select-list как корневые объекты для
; запроса.
	 



		   
		    
	 (
    
    ( (make-loaders e-loader select-list)))
    (

=> make-loaders

(defun sql-query (query)
  ("SELECT"
   (select-list-of query)
   "FROM"
   (tquery-mappings-of query)


;; у любого выражения (expression) загружается только результат -
;; значение
;;
;; у любого связывания (binding), объхекта или ассоциций загружается
;; объект

;; Результат запроса обрабатывается двумя загрузчиками:
;; 1. объектный загрузчик (object loader).
;; Нужны все данные по объектам из БД
;; ассциации загружаются также, объектным загрузчиком
;; 2. загрузчик результата выражения (expression loader)
;; Нужна только часть, участвующая в выражении.
;; Надо собрать информацию о привязках используемых 
;; для в запросе.

;; когда надо назначать псевдонимы таблицам? Ведь они важны для
;; загрузки.

;; Для загрузки объекта - создется загрузочник объекта в котором вся
;; эта информация есть. Для загрузки значения выражения требуются
;; только связи между таблицами.

;; Какую информацию необходимо сообщить загрузчику значения?

;; Сперва создаем отображение запроса (query-mappings).  Результатом
;; будет список объектов query-mapping. Они понадобятся для построения
;; загрузочников. В отображении участвуют связывания.

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

