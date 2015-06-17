(in-package #:cl-db)

;; для библиотеки подключений (унификация)
(define-database-interface postgresql-postmodern
  (:open-connection #'(lambda (&rest args)
			(apply #'cl-postgres:open-database args)))
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute
   #'(lambda (connection name &rest params)
       (cl-postgres:exec-prepared connection name params
				  'cl-postgres:alist-row-reader))))

(define-database-interface name &options options)
(define-mapping-schema name) - определяем схему (текущая)
;; нет необходимости, компиляция будет ленивой
;; (compile-mapping-schema)- копилируем текущую схему отображения
(with-session (connection-args) &body body) - Контекст для работы с объектами из БД.
(db-persist object) - добавление объектов контекст работы с БД
(db-remove object) - удаление коневого объекта из контекста
(db-query (&rest bindings) (&rest options) (&rest cortesian-product))

;; Макрос для запросов объектов из БД.
;; Содержит механизм ленивой компиляции отображения.
;; Сам компилируется в вызов обращения к БД с запросом и 
;; создание объекта загрузчика результата.

;;(defclass clos-transaction ()
;;  ((clos-session :initarg :clos-session :reader clos-session-of)
;;   (new-objects :initform (list) :accessor new-objects-of)
;;   (objects-snapshots :initarg :objects-snapshots :reader object-snapshots)))

;;(defun begin-transaction (&optional (session *session*))
;;  (make-instance 'clos-transaction :clos-session clos-session
;;		 :objects-snapshots (make-snapshot (list-loaded-objects session)
;;						   (mappings-of session))))

;;(defun persist (object &optional (transaction *transaction*))
;;  (when (not (loaded-p object (clos-session-of transaction)))
;;    (push (new-objects-of transaction) object)))

;;(defun load (mapping row))

;;  (query-db (connection-of session)
;;	    (get-mapping class-name)

;;(defun db-get (class-name &rest primary-key)

(defun compute-fetch-joins (class-mapping joined-fetch)
  (mapcar #'(lambda (joined-fetch)
	      (make-instance 'left-outer-join
			     :foreign-key (foreign-key-of
					   (get-reference-mapping class-mapping
								  joined-fetch))))
	  joined-fetch))

(defclass joined-fetch ()
  ((reference-reader :initarg :reference-reader
		     :reader reference-reder-of)
   (joined-fetch :initarg :joined-fetch
		 :reader joine-fetch-of)))

(defun compute-joins (table-reference class-mapping)
  (append
   (compute-subclasses-joins table-reference
			     (superclasses-mappings-of class-mapping))
   (compute-superclasses-joins table-reference
			       (subclasses-mappings-of class-mapping))))

(defgeneric make-query-string (query))

;;(defmethod make-query-string ((query select-query))
;;  (format nil "SELECT ~{~a~^, ~} FROM ~a 

;; execute

;;defclass result - hash-table of table-rows on foreign0keys
;; primary-key inheritance
;;(defclass db-query ()
;;  ((table :initarg :table :reader table)
;;   (foreign-keys :initarg :foreign-keys :reader foreign-keys)))

;;(defclass table-row ()
;;  ((values :initarg :values
;;	   :reader values-of)
;;   (foreign-key-tables-rows :initarg :foreign-key-tables-rows
;;			    :reader foreign-key-tables-rows-of)))

;; load

;; наследование и инициализация слотов объект какого класса создавать?
;; При условии, что нет абстрактных классов объекты по иерархии
;; загружаются снизу-вверх из исключенных записей нижнего уровня

;; скорее всего функция будет рекурсивной (особенно если fetch будет с Join'ами)
;;(defun load (session class-mapping fetch result)
;;  (let ((object (allocate-instance (mapped-class-of class-mapping))))
;;    (cache object session)
;;    (maphash #'(lambda (slot-name slot-mapping)
;;		 (

(lift:addtest (test-mappings) object-loaders
	      (with-session (session)
		(assert (= (length
			    (subclass-object-loaders-of
			     (make-instance 'object-loader
					    :class-mapping (get-class-mapping session
									      (find-class 'project)))))
			   0))
		(assert (= (length
			    (subclass-object-loaders-of
			     (make-instance 'object-loader
					    :class-mapping (get-class-mapping session
									      (find-class 'project-member)))))
			   1))))

(lift:addtest (test-mappings) making-simple-select
	      (with-session (session)
		(assert
		 (string= (make-query-string
			   (make-select-query
			    (get-class-mapping session (find-class 'project))))
			  "SELECT t_1.id, t_1.name, t_1.begin_date FROM projects as t_1"))))


(lift:addtest (test-mappings) making-select-with-inheritance
	      (with-session (session)
		(assert
		 (string= (make-query-string
			   (make-select-query
			    (get-class-mapping session (find-class 'project-manager))))
			  "SELECT t_1.user_id, t_1.project_id, t_2.user_id, t_2.project_id FROM project_members as t_1 INNER JOIN project_managers as t_2 ON t_1.user_id = t_2.user_id AND t_1.project_id = t_2.project_id"))))

;;;;;;;;;;;;;;;;;;

(define-mapping-schema projects-managment)

(define-class-mapping (user "users")
    ((:primary-key "id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (login (:value ("login" "varchar")))
  (password (:value ("password" "varchar")))
  (project-managments (:one-to-many project-managment "user_id")
		      #'(lambda (&rest roles)
			  (alexandria:alist-hash-table
			   (mapcar #'(lambda (role)
				       (cons (project-of role) role))
				   roles)))
		      #'alexandria:hash-table-values)
  (project-participations (:one-to-many project-managment "user_id")
			  #'(lambda (&rest roles)
			      (alexandria:alist-hash-table
			       (mapcar #'(lambda (role)
					   (cons (project-of role) role))
				       roles)))
			  #'alexandria:hash-table-values))

(define-class-mapping (project-participation "project_memebers")
    ((:primary-key "project_id" "user_id"))
  (project (:many-to-one project "project_id"))
  (user (:many-to-one user "user_id")))

(define-class-mapping (project-managment "project_managers")
    ((:superclasses project-participation))
  (project-participation "project_id" "user_id"))

(define-class-mapping (project "projects")
    ((:primary-key "id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (begin-date (:value ("begin_date" "date")))
  (objects (:many-to-one project-root-object "project_id"))
  (document-directories
   (:many-to-one root-document-directory "project_id"))
  (document-registrations
   (:one-to-many document-registration "project_id")
   #'(lambda (&rest registrations)
       (alexandria:alist-hash-table
	(mapcar #'(lambda (registration)
		    (cons (document-of registration)
			  registration))
		registrations)))
   #'alexandria:hash-table-values)
  (project-members (:one-to-many project-member ("project_id"))
		   #'(lambda (&rest roles)
		       (alexandria:alist-hash-table
			(mapcar #'(lambda (role)
				    (cons (user-of role) role))
				roles)))
		   #'alexandria:hash-table-values))
;;  (main-plan (:many-to-one project-plan "main_plan_id"))
;;  (project-plans (:one-to-many project-plan "project_id"))
;;  (tasks (:one-to-many task "project_id")))
;;  (axes (:one-to-many object-axis "project_id" "object_id")))

(define-class-mapping (project-object "project_objects")
    ((:primary-key "project_id" "id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (project (:many-to-one project "project_id"))
  (subobjects (:one-to-many project-subobject "project_id" "parent_id")))

(define-class-mapping (project-root-object "project_root_objects")
    ((:superclasses project-object)))

(define-class-mapping (project-subobject "project_subobjects")
    ((:superclasses project-object))
  (parent (:many-to-one project-object "parent_id")))

(define-class-mapping (project-task "project_tasks")
    ((:primary-key "project_id" "object_id" "id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (description (:value ("description" "varchar")))
  (object (:many-to-one object "project_id" "object_id")))

;;(define-class-mapping (root-object-axis "root_object_axes")
;;    ((:primary-key "project_id" "root_object_id" "id"))
;;  (id (:value ("id" "uuid")))
;;  (name (:value ("name" "varchar"))))

(define-class-mapping (document-directory "document_directories")
    ((:primary-key "project_id" "id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (project (:many-to-one project "project_id"))
  (document-classes
   (:one-to-many document "project_id" "parent_id"))
  (subdirectories
   (:one-to-many document-subdirectory "project_id" "parent_id")))

(define-class-mapping
    (document-root-directory "document_class_root_directories")
    ((:superclasses document-directory)))

(define-class-mapping
    (document-subdirectory "document_class_subdirectories")
    ((:superclasses document-class-directory))
  (parent-directory (:many-to-one document-directory "parent_id")))

(define-class-mapping (document "documents")
    ((:primary-key "project_id" "id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (project (:many-to-one project "project_id"))
  (parent-directory (:many-to-one document-directory "parent_id")))

(define-class-mapping (document-registration "project_documents")
    ((:primary-key "project_id" "document_id"))
  (id (:value ("id" "uuid")))
  (name (:value ("name" "varchar")))
  (document (:many-to-one document "document_id"))
  (files (:one-to-many document-file "document_id")))

(define-class-mapping (document-file "document_files")
    ((:primary-key "project_id" "document_id"
		   "large_object_descriptor"))
  (name (:value ("name" "varchar")))
  (mime-type (:value ("mime_type" "varchar")))
  (large-object-descriptor
   (:value ("large_object_descriptor" "integer"))))

(with-session ((:mapping-schema project-managment)
	       (:database-interface postgresql-postmodern)
	       (:connection-args "projects" "makarov" "zxcvb" "localhost"))
  (do-query ...)
  (persist-object ...)
  (remove-object ...))

(defparameter *default-mapping-schema* 'project-managment)
(defparameter *default-database-interface* 'postgresql-postmodern)
(defparameter *default-connection-args*
  '("projects" "makarov" "zxcvb" "localhost"))

(with-session ()
  (do-query ...)
  (persist-object ...)
  (remove-object ...))

(defun smthn (name password)
  (with-session ((:connection-args "projects" name password "localhost"))
    ...))

Structure of a Query

Simple Expressions

(define-query list-cats (cat))

(list-cats) => list all cats
;; OR
(db-read 'cat)

;;;;;;;;;;;;;;;;;;;

IList<Cat> cats =
    session.QueryOver<Cat>()
        .Where(c => c.Name == "Max")

Macro:

(define-query (get-cat name)
    (cat)
  (:where (:eq (name-of cat) name)))

(get-cat "Max")

Function:

(db-read 'cat :where #'(lambda (cat)
			 (restrict
			  (property root #'name-of) :equal "Max")))

Additional Restrictions

var catNames = session.QueryOver<Cat>()
        .WhereRestrictionOn(c => c.Age).IsBetween(2).And(8)
        .Select(c => c.Name)
        .OrderBy(c => c.Name).Asc
        .List<string>();

(define-query (get-cat-names min-age max-age)
    (cat)
  (:select (name-of cat))
  (:where
   (:between (age-of cat) min-age max-age))
  (:order-by
   (:ascending (name-of cat))))

(get-cats 2 8)

(db-read 'cat
	 :select #'(lambda (cat)
		     (property cat #'name-of))
	 :where #'(lambda (cat)
		     (restrict (property cat #'age-of)
			       :more-than-or-equal 2
			       :less-than-or-equal 8))
	 :order-by #'(lambda (cat)
		       (ascending (property cat #'name-of))))

var cats =
    session.QueryOver<Cat>()
        .Where(c => c.Name == "Max")
        .And(c => c.Age > 4)
        .List();

(define-query (get-cats name min-age)
    (cat)
  (:where
   (:eq (name-of cat) name)
   (:> (age-of cat) min-age)))

(get-cats "Max" 4)

(db-read 'cat :where #'(lambda (cat)
			 (conjunction
			  (restrict
			   (property root #'name-of) :equal "Max")
			  (restrict
			   (property root #'age-of) :more-than-or-equal 8))))

(db-read 'cat :where #'(lambda (cat)
			 (values
			  (restrict
			   (property root #'name-of) :equal "Max")
			  (restrict
			   (property root #'age-of) :more-than-or-equal 8))))

Associations

IQueryOver<Cat,Kitten> catQuery =
    session.QueryOver<Cat>()
        .JoinQueryOver(c => c.Kittens)
            .Where(k => k.Name == "Tiddles");

(db-read 'cat
	 :join #'(lambda (cat)
		   (join cat #'kittens-of :alias :kitten))
	 :where #'(lambda (cat &key kitten)
		     (restrict
		      (property kitten #'name-of) :equal "Tiddles")))

=> (list cat kitten)

(define-query (find-kittens kitten-name)
    ((cat cat)
     (kitten (kittens-of cat)))
  (:select kitten)
  (:where
   (:eq (name-of kitten) kitten-name)))

(parents "Tiddles") => cats

Projections

IList selection =
    session.QueryOver<Cat>()
        .Select(
            c => c.Name,
            c => c.Age)
        .List<object[]>();

(define-query name-and-age (cat)
  (:select
   (name-of cat)
   (age-of cat)))

(db-read 'cat :select #'(lambda (cat)
			  (values
			   (property cat #'name-of)
			   (property cat #'age-of))))
IList selection =
    session.QueryOver<Cat>()
        .Select(Projections.ProjectionList()
            .Add(Projections.Property<Cat>(c => c.Name))
            .Add(Projections.Avg<Cat>(c => c.Age)))
        .List<object[]>();

(define-query average-age (cat)
  (:select
   (name-of cat)
   (:avg (age-of cat))))

(db-read 'cat :select #'(lambda (cat)
			  (values
			   (name-of cat)
			   (aggregation 'avg (age-of cat)))))

Subqueries

QueryOver<Cat> maximumAge =
    QueryOver.Of<Cat>()
        .SelectList(p => p.SelectMax(c => c.Age));

IList<Cat> oldestCats =
    session.QueryOver<Cat>()
        .WithSubquery.WhereProperty(c => c.Age).Eq(maximumAge)
        .List();

(define-query maximum-age ((cat cat)
			   (max-age-cat cat))
  (:select cat)
  (:having
   (:eq
    (age-of cat)
    (:max
     (age-of max-age-cat)))))

(db-read '(cat cat)
	 :having #'(lambda (cat max-age-cat)
		     (let ((max-age
			    (agggregation
			     (property max-age-cat #'age-of))))
		       (restrict
			(property cat #'age-of) :equal max-age))))

Limit, offset

(define-query limit-offset (cat)
  (:limit 10)
  (:offset 100))

(db-read 'cat :limit 100 :offset 100)

Fetching

(define-query cats (cat)
  (:fetch (kittens-of cat)))

(db-read 'cat :fetch #'(lambda (cat)
			 (fetch cat #'kittens-of)))

Single instance

(define-query get-cat (cat)
  (:where (:eq (id-of cat) 35))
  (:single t))

(db-read 'cat :singlep t
	 :where (lambda (cat)
		  (restrict
		   (property-of cat #'id-of) :equal 35)))

;; Проблема рекурсивных ключей. Идентификатор объекта не может
;; идентифицироваться в дереве своим родителем. В противном случае
;; необходимо рекурсиное построение запроса.

If PK (user id and project id)

(define-query <>
    ((project-manager project-manager)
     (project
      (project-of project-manager))
     (user
      (user-of project-manager))) ; joins
  (:select project-manager)
  (:where
   (:eq (id-of user) 1)
   (:eq (id-of project) 1))
  (:single t))

(db-read 'project-manager
	 :join #'(lambda (project-manager)
		   (values
		    (join project-manager #'project-of :alias :project)
		    (join project-manager #'user-of :alias :user)))
	 :select #'(lambda (project-manager &key project user) ;; include joins in selection
		     (values project-manager project user)))
   => (list-of (list project-manager project user))

(defmacro db-query (bindings &body clauses)
  (

(load
 () <- loader
 (execute
  (ensure-prepared session
		   "SELECT t_1.project_id, t_1.user_id
   FROM project_managers as t_1
  INNER JOIN projects as t_2
     ON t_1.project_id = t_2.id
  INNER JOIN users as t_3
     ON t_1.user_id = t_3.user_id
  WHERE t_2.id = $1
    AND t_3.id = $2")
 1 1)


;; Criteria API

ICriteria crit = sess.CreateCriteria(typeof(Cat));
crit.SetMaxResults(50);
List cats = crit.List();

(db-read 'cat :limit 50)

IList cats = sess.CreateCriteria(typeof(Cat))
    .Add(Expression.Like("Name", "Fritz%"))
    .Add(Expression.Between("Weight", minWeight, maxWeight))
    .List();

(db-read 'cat :where #'(lambda (cat)
			 (values
			  (restrict
			   (property cat #'name-of) :like "Fritz%")
			  (restrict (property cat #'weignt-of)
				    :more-than-or-equal min-weight
				    :less-than-or-equal max-weight))))

(db-read 'cat :where #'(lambda (cat)
			 (conjunction
			  (restrict
			   (property cat #'name-of) :like "Fritz%")
			  (restrict (property cat #'weignt-of)
				    :more-than-or-equal min-weight
				    :less-than-or-equal max-weight))))

IList cats = sess.CreateCriteria(typeof(Cat))
    .Add(Expression.Like("Name", "Fritz%"))
    .Add(Expression.Or(
        Expression.Eq("Age", 0),
        Expression.IsNull("Age"))).List();

(db-read 'cat :where #'(lambda (cat)
			 (conjunction
			  (restrict
			   (property cat #'name-of) :like "Fritz%")
			  (disjunction
			   (restrict
			    (property cat #'age-of) :equal 0)
			   (restrict
			    (property cat #'age-of) :is #'null)))))

IList cats = sess.CreateCriteria(typeof(Cat))
    .Add( Expression.In( "Name", new String[] { "Fritz", "Izi", "Pk" } ) )
    .Add( Expression.Disjunction()
        .Add( Expression.IsNull("Age") )
    	.Add( Expression.Eq("Age", 0 ) )
    	.Add( Expression.Eq("Age", 1 ) )
    	.Add( Expression.Eq("Age", 2 ) )
    ) )
    .List();

(db-read 'cat :where (lambda (cat)
		       (conjunction
			(restrict (property cat #'name-of)
				  :member (list "Fritz" "Izi" "Pk"))
			(disjunction
			 (restrict
			  (property cat #'age-of) :null t)
			 (restrict
			  (property cat #'age-of) :equal 0)
			 (restrict
			  (property cat #'age-of) :equal 1)
			 (restrict
			  (property cat #'age-of) :equal 2)))))

;; биарные операции применимы только для значений схожего типа
;; унарные применимы только для одного значения или выражения
;; таким образом возможно построение совокупности операций для стобцов
Expressions

(define-expression (:eq) (column-name column-name) ;; ???
  (format "\{ ~a+ \}" obj1 obj2)))

(define-expression :+ (&rest expressions)
  (format "\{ ~a+ \}" expressions))

(define-expression :sum (column)
  (format "sum(~a)" column))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(define-class-mapping (user "users")
    ((:primary-key "id"))
  (id (:value ("id" "integer")))
  (name (:value ("name" "varchar")))
  (login (:value ("login" "varchar")))
  (password (:value ("password" "varchar")))
  (project-participations
   (:one-to-many project-participation "user_id")
			 #'(lambda (&rest roles)
			     (reduce #'(lambda (table role)
					 (setf (gethash (project-of role) table) role)
					 table)
				     roles
				     :initial-value (make-hash-table :size (length roles))))
			 #'alexandria:hash-table-values)
  (project-managments
   (:one-to-many project-managment "user_id")
   #'(lambda (&rest roles)
       (reduce #'(lambda (table role)
		   (setf (gethash (project-of role) table) role)
		   table)
	       roles
	       :initial-value (make-hash-table :size (length roles))))
   #'alexandria:hash-table-values))

(define-class-mapping (project "projects")
    ((:primary-key "id"))
  (id (:value ("id" "integer")))
  (name (:value ("name" "varchar")))
  (begin-date (:value ("begin_date" "timestamp")))
  (project-roles
   (:one-to-many project-role "project_id")
   #'(lambda (&rest roles)
       (reduce #'(lambda (table role)
		   (setf (gethash (user-of role) table) role)
		   table)
	       roles
	       :initial-value (make-hash-table :size (length roles))))
   #'alexandria:hash-table-values))

(define-class-mapping (project-role "project_roles")
    ((:primary-key "project_id" "user_id"))
  (project (:many-to-one project "project_id"))
  (user (:many-to-one user "user_id")))
  
(define-class-mapping (project-participation "project_participations")
    ((:superclasses project-role)))

(define-class-mapping (project-managment "project_managments")
    ((:superclasses project-role)))

;; Загрузка

(let (result-alists loaders)
  (mapcar #'(lambda (row)
	      (load-object loader resu

;; 1. Создаем query-tree - анализируем что нужно загрузить (какие
;; связи: ассоциации и наследование).

;; 2. Создаем table-trees - те же отношения (1), но как отношения
;; таблиц (со связями: left joins и inner joins).

;; 3. Создаем select-list - список выражений, но уже по table-tree
;; (загрузка результата связана с этим списком).

;; 4. Аналогичено с select-list создаем where-claue, order-by-clause,
;; having-clause.

;; добавить fetch-also

(defclass table-reference ()
  ((root-binding :initarg :root-binding :reader root-binding-of)
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

(let ((curriculum
       (bind-root 'curriculum))
      (subjects
       (bind-reference curriculum #'subjects-of)))
  (make-query (join-fetch curriculum
			  (join-fetch subjects #'semester-works-of))))

(db-read 'curriculum
	 :select #'(lambda (curriculum)
		     (values
		      (property curriculum #'id-of)
		      (property curroculum #'name-of)
		      (expression
		       (:count
			(property curriculum #'version-of)))))
	 :join #'(lambda (curriculum)
		   (join curriculum #'direction-of :alias direction))
	 :where #'(lambda (curriculum &key direction)
		    (declare (ignore curriculum))
		    (expression
		     (:eq (id-of direction) <direction-id>)))
	 :fetch #'(lambda (curriculum)
		    (values
		     (fetch curriculum #'semesters-of
			    :fetch #'(lambda (semester)
				       (fetch semester #'subject-parts-of)))
		     (fetch curriculum #'subject-courses-of
			    :fetch #'(lambda (subject-course)
				       (fetch subject-course #'course-parts-of))))))

;; при запросе:
1. Информация о наследовании
2. список слотов-свойств (после наслседованиЯ логически-ближайшая информация)
3. список ссылок для join reference (указать структурно типы отображения)
4. Список колонок для select-list'а, 

(<- class-mapping class-name table-name)
(<- primary-key table-name primary-key)

(<- superclass class-name superclass-name foreign-key)
(<- (superclass ?x ?y ?foreign-key)
    (superclass ?y ?z foreign-key))

(<- value-mapping class-name slot-name columns deserializer serializer)
(<- many-to-one class-name slot-name reference-class-name foreign-key)
(<- one-to-many class-name slot-name reference-class-name foreign-key
    deserializer serializer)
    
(?- (or (class-mapping class-name ?table-name)
	(iheritance class-name ?superclass-name ?foreign-key)))

(defun make-schema (class-mapping &rest class-mappings)
  (reduce #'(lambda (schema mapping-fn)
	      (list* (funcall mapping-fn schema) schema))
	  (list* class-mapping class-mappings)
	  :initial-value nil))

(defun compute-inheritance (schema class-name &rest foreign-key)
  (list* (assoc class-name schema) foreign-key))

(defun map-class (class-name table-name primary-key
		  &key slots superclasses)
  (let ((pk-columns
	 (multiple-value-list
	  (funcall primary-key))))
    #'(lambda (schema)
	(values
	 #'(lambda (schema)
	     (
	 #'(lambda (schema)
	     (list*
	      (list* class-name
		     (list* table-name pk-columns)
		     (mapcar #'(lambda (superclass-mappings-fn)
				 (funcall superclass-mappings-fn schema))
			     (multiple-value-list
			      (funcall superclasses (first schema)))))
	      (remove class-name (first schema) :key #'first)))
	 
(defun map-superclass (class-name &rest columns)
  #'(lambda (schema)
      (list* (assoc class-name schema) foreign-key)))

(defun map-slot (slot-name mapping-fn
		 &optional
		   (serialize-fn #'list)
		   (deserialize-fn #'value))
  #'(lambda (schema class-name)
      (funcall mapping-fn schema class-name
	       serialize-fn deserialize-fn)))

1. inheritance
2. tables
2. properties
3. references

(defun map-property (slot-name column-name column-type)
  #'(lambda (schema class-name serialize-fn deserialize-fn)
      schema))
;;      (let ((properties (assoc class-name (second schema))))
;;	(list*
;;	 (acons slot-name
;;		(list
;;		 (cons column-name column-type)
;;		 (cons serialize-fn deserialize-fn))
;;		(remove slot-name properties :key #'first))
;;	 (remove properties properties-schema)))))

(defun many-to-one (slot-name class-name &rest columns)
  #'(lambda (schema class-name serialize-fn deserialize-fn)
      schema))

(defun one-to-many (slot-name class-name &rest columns)
  #'(lambda (schema class-name serialize-fn deserialize-fn)
      schema))

;;  #'(lambda (schema class-name)
;;      (let ((class-mapping (assoc class-name schema)))
;;	(acons class-name
;	       
;;	       (remove class-name schema :key #'first)

(define-session postgresql ()
  (:open-connection #'open-database)
  (:close-connection #'close-database)
  (:prepare #'prepare)
  (:exec-prepared #'exec-prepared))

(define-mapping (user ("users" "id"))
    ((:id (:column "id" "uuid")
	  #'(lambda (user id)
	      (setf (slot-value user 'id) id)
	      user)
	  #'id-of)
     (:name (:column "name" "varchar")
	    #'(lambda (user name)
		(setf (slot-value user 'name) name)
		user)
	    #'name-of)
     (:login (:column "login" "varchar")
	     #'(lambda (user login)
		 (setf (slot-value user 'login)
		       login)
		 user)
	     #'login-of)
     (:password (:column "password" "varchar")
		#'(lambda (user password)
		    (setf (slot-value user 'password)
			  password)
		    user)
		#'password-of)
     (:project-managments
      (:one-to-many project-managment "user_id")
      #'(lambda (user &rest roles)
	  (setf
	   (slot-value user 'project-managments)
	   (alexandria:alist-hash-table
	    (mapcar #'(lambda (role)
			(cons (project-of role)
			      role))
		    roles)))
	  user)
      #'project-managments-of)
     (:project-participations
      (:one-to-many project-participation "user_id")
      #'(lambda (&rest roles)
	  (setf
	   (slot-value user 'project-participations)
	   (alexandria:alist-hash-table
	    (mapcar #'(lambda (role)
			(cons (project-of role) role))
		    roles)))
	  user)
      #'project-participations-of))
  (allocate-instance 'user))

(define-mapping (project-participation
		 ("project-participations" "user_id" "project_id"))
    ((:project (:many-to-one project "project_id")
	       #'(lambda (project-participation project)
		   (setf (slot-value project-participation 'project)
			 project)
		   project-participation)
	       #'project-of)
     (:user (:many-to-one user "user_id")
	    #'(lambda (project-participatio user)
		(setf (slot-value project-participation 'user)
		      user)
		project-participation)
	    #'user-of))
  (allocate-instance 'project-participation))

(define-mapping (project-managment
		 ("project-participations" "user_id" "project_id")
		 (project-participation "user_id" "project_id"))
    ()
  (allocate-instance 'project-managment))
		  
(define-mapping (project ("projects" "id"))
    ((:id (:column "id" "uuid")
	  #'(lambda (user id)
	      (setf (slot-value user 'id) id)
	      user)
	  #'id-of)
     (:name (:column "name" "varchar")
	    #'(lambda (user name)
		(setf (slot-value user 'name) name)
		user)
	    #'name-of)
     (:project-managments
      (:one-to-many project-managment "project_id")
      #'(lambda (project &rest roles)
	  (setf
	   (slot-value project 'project-managments)
	   (alexandria:alist-hash-table
	    (mapcar #'(lambda (role)
			(cons (project-of role) role))
		    roles)))
	  project)
      #'project-managments-of)
     (:project-participations
      (:one-to-many project-participation "project_id")
      #'(lambda (project &rest roles)
	  (setf
	   (slot-value project 'project-participations)
	   (alexandria:alist-hash-table
	    (mapcar #'(lambda (role)
			(cons (project-of role) role))
		    roles)))
	  project)
      #'project-participations-of))
  (allocate-instance 'project))

(define-mapping (map-name
		 ("table-name" "primary" "key")
		 (joined-mapping "foreign" "key"))
    (allocate-instance 'class-name)
  (value-1
   (:property "column-name")
   (:not-null t)
   (:reader #'value-1-of)
   (:writer #'(lambda 
  (value-2
   (:many-to-one another-map "foreign" "key")
   (:not-null t))
  (value-3
   (:one-to-many map-2 "foreign" "key")
   (:cascade :delete-orphan)))
  (
))))

(define-class-mapping user-map
    (("users" "id"))
  (id (:property "id" "uuid"))
  (name (:property "name" "varchar"))
  (login (:property "login" "varchar"))
  (password (:property "password" "varchar"))
  (project-managments
   (:one-to-many project-managment "user_id")
   #'(lambda (&rest roles)
       (alexandria:alist-hash-table
	(mapcar #'(lambda (role)
		    (cons (project-of role) role))
		roles)))
   #'alexandria:hash-table-values)
  (project-participations
   (:one-to-many project-participation "user_id")
   #'(lambda (&rest roles)
       (alexandria:alist-hash-table
	(mapcar #'(lambda (role)
		    (cons (project-of role) role))
		roles)))
   #'alexandria:hash-table-values))

(define-class-mapping project-participation-map
    (("project_memebers" "project_id" "user_id"))
  (project (:many-to-one project "project_id"))
  (user (:many-to-one user "user_id")))

(define-class-mapping project-managment-map
    (("project_managers" "project_id" "user_id")
     (project-participation "project_id" "user_id")))

(define-class-mapping project-map
    (("projects" "id"))
  (id (:property "id" "uuid"))
  (name (:property "name" "varchar"))
  (begin-date (:property "begin_date" "date"))
  (project-members
   (:one-to-many project-participation "project_id")
   #'(lambda (&rest roles)
       (alexandria:alist-hash-table
	(mapcar #'(lambda (role)
		    (cons (user-of role) role))
		roles)))
   #'alexandria:hash-table-values))

(define-mapping-schema projects-managment ()
  (user user-map)
  (project-participation project-participation-map)
  (project-managment project-managment-map)
  (project project-map)))

(defmacro define-class-mapping (name ((table-name &rest primary-key)
				      &rest inheritance-mappings)
				&body slot-mappings)
  `(setf
    (get (quote ,name) 'class-mapping)
    (make-instance 'class-mapping
		   :table-name ,table-name
		   :primary-key (quote ,primary-key)
		   :properties (compute-properties
				(quote ,slot-mappings))
		   :one-to-many (compute-one-to-many
				 (quote ,slot-mappings))
		   :many-to-one (compute-many-to-one
				 (quote ,slot-mappings))
		   :superclass-mappings (compute-superclass-mappings
					 (quote ,inheritance-mappings)))))

(defmacro define-mapping-schema (name ()
				 (class-name
				  &optional (mapping-name class-name))
				 &rest rest-mappings)
  `(setf
    (get (quote ,name) 'mapping-schema)
    (reduce #'(lambda (result class-and-mapping)
		(destructuring-bind
		      (class-name &optional (mapping-name class-name))
		    class-and-mapping
		  (acons class-name mapping-name result)))
	    (quote ,rest-mappings)
	    :initial-value (acons (quote ,class-name)
				  (quote ,mapping-name)
				  nil))))

;; reference each mapping-schema for each used class-mapping
;; class-mapping redefinition for each mapping-schema
;; mapping-schema redefinition for each class-mappings
;; generate database schema for each mapping-schema

;; class tree-node (left-node right-node)

(db-read 'tree-node :fetch ;; recursive-fetch
	 #'(lambda (tree-node)
	     (values
	      (fetch tree-node #'left-node-of
		     :recursive tree-node)
	      (fetch tree-node #'right-node-of
		     :recursive tree-node))))
;; все дерево

(let ((property-id <id>))
  (db-read 'proprty-change ;; узелы которые хранят изменения свойств
	   :join #'(lambda (property-change)
		     (join property-change #'property-of :alias :property))
	   :where #'(lambda (property-change &key property)
		      (restrict
		       (property property #'id-of) :equal property-id))))
;; все изменения свойства, какие только есть

;; recursive fetching
	 
(let ((commit-id <id>))
  (db-read 'commit
	   :where #'(lambda (commit)
		      (restrict
		       (property commit #'id-of) :equal commit-id))
	   :fetch #'(lambda (commit)
		     (fetch commit #'property-values-root
			    :fetch #'(lambda (tree-node)
				       (values
					(fetch tree-node #'property-value
					       :fetch #'(lambda (property-value)
							  (fetch property-value
								 #'property-of)))
					(fetch tree-node #'left-node-of
					       :recursive #'(lambda (left-node)
							      (restrict
							       (property left-node #'id-of)
							       :equal
							       (property tree-node #'id-of))))
					(fetch tree-node #'right-node-of
					       :recursive #'(lambda (right-node)
							      (expression
							       (:eq
								(id-of right-node)
								(id-of tree-node)))))))))))

;; recursive joining

(let ((commit-id <id>)
      (object-id <id>))
  (db-read 'commit
	   :join #'(lambda (commit)
		     (values
		      (join commit #'property-values-tree-of
			    :alias :property-value-node
			    :join #'(lambda (tree-node)
				      (values
				       (join tree-node #'property-value
					     :join #'(lambda (property-value)
						       (join property-value #'property-of
							     :alias :property-of-value)))
				       (join tree-node #'left-node-of
					     :recursive #'(lambda (left-node)
							    #Q(eq
							       (id-of tree-node)
							       (id-of left-node))))
				       (join tree-node #'right-node-of
					     :recursive #'(lambda (right-node)
							    #Q(eq
							       (id-of tree-node)
							       (id-of right-node)))))))
		      (join commit #'properties-tree-of
			    :join #'(lambda (tree-node)
				      (values
				       (join tree-node #'object-properties-of
					     :join #'(lambda (object-property-node)
						       (values
							(join object-property #'object-of
							      :where #'(lambda (object)
									 #Q(eq (id-of object) <object-id>)))
							(join object-property-node #'next-node-of
							      :recursive #'(lambda (next-node)
									     #Q(eq
										(id-of object-property-node)
										(id-of next-node)))
							      :join #'(lambda (property-node)
									(join property-node #'property-of
									      :alias :object-property))))))
				       (join tree-node #'left-node-of
					     :recursive #'(lambda (left-node)
							    #Q(eq
							       (id-of tree-node)
							       (id-of left-node))))
				       (join tree-node #'right-node-of
					     :recursive #'(lambda (right-node)
							    #Q(eq
							       (id-of tree-node)
							       (id-of right-node)))))))))
	   :where #'(lambda (commit &key value object-property property-of-value)
		      (declare (ignore property-of-value))
		      (expression
		       (:and
			(:eq (id-of commit) <commit-id>)
			(:eq (id-of property)
			     (id-of object-property)))))
	   :select #'(lambda (commit &key value object-property property-of-value)
		       (declare (ignore commit object-property property-of-value))
		       (list property property-value))))

;; recursive joining 2
(db-read 'comment 
	 :aux #'(lambda (commit &key value object-property property-of-value) ;; same as where but pass to WHERE condition of WITH clause
		  (declare (ignore value object-property property-of-value))
		  #Q(eq (id-of commit) commit-id))
	 :where #'(lambda (commit &key value object-property property-of-value)
		    (declare (ignore property-of-value))
		    #Q(and
		       (eq (id-of property-of-value)
			   (id-of object-property))
		       (eq (id-of object) <object-id>)))
	 :select #'(lambda (commit &key value object-property property-of-value)
		     (declare (ignore commit object-property property-of-value))
		     (values property value))
	 :join #'(lambda (commit)
		   (values
		    (join commit #'property-values-tree-of
			  :alias :property-value-node
			  :join #'(lambda (tree-node)
				    (values
				     (join tree-node #'property-value
					   :join #'(lambda (property-value)
						     (join property-value #'property-of
							   :alias :property-of-value)))
				     (join tree-node #'left-node-of
					   :recursive #'(lambda (left-node)
							  (expression #'eq
							    (id-of tree-node)
							    (id-of left-node))
							  #E(eq
							     (id-of tree-node)
							     (id-of left-node))))
				     (join tree-node #'right-node-of
					   :recursive #'(lambda (right-node)
							  #E(eq
							     (id-of tree-node)
							     (id-of right-node)))))))
		    (join commit #'properties-tree-of
			  :join #'(lambda (tree-node)
				    (values
				     (join tree-node #'object-properties-of
					   :join #'(lambda (object-property-node)
						     (values
						      (join object-property #'object-of
							    :alias :object)
						      (join object-property-node #'next-node-of
							    :recursive #'(lambda (next-node)
									   #Q(eq
									      (id-of object-property-node)
									      (id-of next-node)))
							    :join #'(lambda (property-node)
								      (join property-node #'property-of
									    :alias :object-property))))))
				     (join tree-node #'left-node-of
					   :recursive #'(lambda (left-node)
							  #Q(eq
							     (id-of tree-node)
							     (id-of left-node))))
				     (join tree-node #'right-node-of
					   :recursive #'(lambda (right-node)
							  #Q(eq
							     (id-of tree-node)
							     (id-of right-node))))))))))))))))))))))))

WITH RECURSIVE parent (pid, id, node_value, left_node_id, right_node_id) AS (
     SELECT tree_nodes.id, tree_nodes.id, tree_nodes.node_value,
     	    left_nodes.tree_node_id, right_nodes.tree_node_id
       FROM tree_nodes
  LEFT JOIN left_nodes
         ON left_nodes.id = tree_nodes.id
  LEFT JOIN right_nodes
         ON right_nodes.id = tree_nodes.id
      UNION ALL
     SELECT chidlren.id, tree_nodes.id, tree_nodes.node_value,
     	    left_nodes.tree_node_id, right_nodes.tree_node_id
       FROM children, tree_nodes
  LEFT JOIN left_nodes
         ON left_nodes.id = tree_nodes.id
  LEFT JOIN right_nodes
         ON right_nodes.id = tree_nodes.id
      WHERE tree_nodes.id = parent.left_node_id
         OR tree_nodes.id = parent.right_node_id);

(defclass car ()
  (car))

(defclass cons (value)
  (cdr))

(db-read 'car
	 :where #'(lambda (car)
		    (expression (:eq (id-of car) 1)))
	 :fetch #'(lambda (car)
		    (fetch car #'cdr :recursive
			   #'(lambda (next-node)
			       (expression
				 (:eq (id-of car)
				      (id-of next-node)))))))

(db-read 'tree-node
	 :where #'(lambda (node)
		    (expression (:eq (id-of node) 1)))
	 :fetch #'(lambda (node)
		    (values
		     (fetch node #'left-node
			    :recursive #'(lambda (left-node)
					  (expression
					    (:eq (id-of left-node)
						 (id-of node)))))
		     (fetch node #'right-node
			    :recursive #'(lambda (right-node)
					   (expression
					     (:eq (id-of right-node)
						  (id-of node))))))))
