(in-package #:cl-db)

:define-mapping-schema - определяем схему
:use-mapping-schema - переключаемся на текущую схему отображения
:make-configuration
:with-session
Контекст для работы с объектами из БД.
:db-persist
:db-remove
:db-query
Макрос для запросов объектов из БД.
Содержит механизм ленивой компиляции отображения.
Сам компилируется в вызов обращения к БД с запросом и 
создание объекта-загрузчика результата.


(setf cl-db:*default-configuration*
      (make-configuration
       :open-connection #'(lambda ()
			    (cl-postgres:open-database "projects"
						       "makarov" "zxcvb"
						       "localhost"))
       :close-connection #'cl-postgres:close-database
       :prepare #'cl-postgres:prepare-query
       :execute #'(lambda (connection name &rest params)
		    (cl-postgres:exec-prepared connection name params
					       cl-postgres:alist-row-reader))
       :list-metadata #'cl-db:list-postgresql-metadata))

;; сделать db-read макросом? или оставить как функцию, а переделывать выражение в with-session?
;; осталось написать

;;(make-instance 'registry
;;	       :mappers (list (make-instance 'mapper :class (find-class 'project)
;;					     :select-all '(:select id name :from projects)
;;					     :select

;;(defvar *session*)

;;(defun db-read (&key all)
;;  (db-find-all all))

;;(defun db-find-all (session class-name)
;;  (let ((class-mapping (gethash class-name (mappings-of session))))
;;    (map 'list #'(lambda (row)
;;		   (load row class-mapping session))
;;	 (execute (connection-of session)
;;		  (make-select-query (table-of class-mapping))))))

;;(defun make-select-query (table)
;;  (let ((table-name (name-of table)))
;;    (format nil "SELECT ~{~a~^, ~} FROM ~a"
;;	    (map 'list #'(lambda (column)
;;			   (format nil "~a.~a" table-name (name-of column)))
;;		 (columns-of table)) table-name)))

;;(defclass clos-transaction ()
;;  ((clos-session :initarg :clos-session :reader clos-session-of)
;;   (new-objects :initform (list) :accessor new-objects-of)
;;   (objects-snapshots :initarg :objects-snapshots :reader object-snapshots)))

;;(defun begin-transaction (&optional (session *session*))
;;  (make-instance 'clos-transaction :clos-session clos-session
;;		 :objects-snapshots (make-snapshot (list-loaded-objects session)
;;						   (mappings-of session))))

;;(defun list-loaded-objects (session)
  

;;(defun persist (object &optional (transaction *transaction*))
;;  (when (not (loaded-p object (clos-session-of transaction)))
;;    (push (new-objects-of transaction) object)))



;;(defgeneric db-query (connection query))

;;(defun load (mapping row))

;;  (query-db (connection-of session)
;;	    (get-mapping class-name)

;;(defun db-get (class-name &rest primary-key)

;;(with-session (*session*)
;;  (db-read :all 'project))

;;(defun db-find-all (session class &rest fetch) 
;;  (let* ((class-mapping (get-class-mapping session class))
;;	 (result (execute (make-select-query class-mapping fetch)
;;			  (connection-of session))))
;;    (map 'list #'(lambda (table-row)
;;		   (load session class-mapping fetch table-row))
;;	 table-rows)))

;;(defun db-read (&key all)
;;  (db-find-all *session* (find-class all)))

(defclass table-expression ()
  ((table-reference :initarg :table-reference
		    :reader table-reference-of)
   (joins :initarg :joins
	  :reader joins-of)))

(defclass join (table-expression)
  ((column-pairs :initarg :column-pairs
		 :reader column-pairs-of)))

(defclass inner-join (join) ())

(defclass left-outer-join (join) ())

(defclass select-query ()
  ((table-expression :initarg :table-expression
		     :reader table-expression-of)
   (select-list-items :initarg :select-list-items
		      :reader select-list-items-of)))

(defun compute-column-pairs (table foreign-key)
  (mapcar #'cons
	  (primary-key-of table)
	  (alexandria:hash-table-keys (columns-of foreign-key))))

(defun compute-superclasses-joins (table-reference superclass-mappings)
  (let ((table (table-of table-reference)))
    (mapcar #'(lambda (superclass-mapping)
		(let* ((superclass-table (table-of superclass-mapping))
		       (superclass-table-reference (make-instance 'table-reference
								  :table superclass-table)))
		  (cons (make-instance 'inner-join
				       :table-reference table-reference
				       :joined-table-reference superclass-table-reference
				       :column-pairs (compute-columns-pairs superclass-table
									    (get-foreign-key table
											     (primary-key-of table)
											     superclass-table)))
			(compute-joins superclass-table-reference superclass-mapping))))
	    superclass-mappings)))

(defun compute-subclasses-joins (table-reference subclasses-mappings)
  (let ((table (table-of table-reference)))
    (mapcar #'(lambda (subclass-mapping)
		(let* ((subclass-table (table-of subclass-mapping))
		       (subclass-table-reference (make-instance 'table-reference
								:table subclass-table)))
		  (cons (make-instance 'left-outer-join
				       :table-reference table-reference
				       :joined-table-reference subclass-table-reference
				       :column-pairs (compute-columns-pairs subclass-table
									    (get-foreign-key subclass-table
											     (primary-key-of subclass-table)
											     table)))
			(compute-class-joins superclass-mapping))))
	    subclasses-mappings)))

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

;; наследование и инициализация слотов
;; объект какого класса создавать? При условии, что нет абстрактных классов
;; объекты по иерархии загружаются снизу-вверх из исключенных записей нижнего уровня


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

(define-configuration ()
    (:open-connection #'(lambda ()
			  (cl-postgres:open-database "projects"
						     "makarov" "zxcvb"
						     "localhost")))
  (:close-connection #'cl-postgres:close-database)
  (:prepare #'cl-postgres:prepare-query)
  (:execute #'(lambda (connection name &rest params)
		(cl-postgres:exec-prepared connection name params
					   cl-postgres:alist-row-reader))))

(with-session ()
  (do-query ...)
  (persist-object ...)
  (remove-object ...))

Structure of a Query

Simple Expressions

(db-query cat) => list all cats

=>

(let ((cat (make-root 'cat mapping-schema)))
  (db-list cat))

(with-session (:mapping mapping-name :configuration session-config)
  (db-query cat))

IList<Cat> cats =
    session.QueryOver<Cat>()
        .Where(c => c.Name == "Max")

Macro:

(db-query ((cat cat))
    ((:where (:eq (name-of cat) "Max"))))

Function:

(make-query
 (make-root 'cat :where (expression cats := #'name-of name)))

(let ((cat (make-root 'cat)))
  (make-query cat
	      :where (expression := (slot-of cats #'name-of) "Max")))

Additional Restrictions

var catNames = session.QueryOver<Cat>()
        .WhereRestrictionOn(c => c.Age).IsBetween(2).And(8)
        .Select(c => c.Name)
        .OrderBy(c => c.Name).Asc
        .List<string>();

(db-query ((cat cat))
    ((:where
      (:between (age-of cat) 2 8))
     (:order-by
      ((name-of cat) :asc)))
  (name-of cat))

(make-query 'cat
	    :select #'name-of
	    :where (expression :betweenp #'age-of 2 8)
	    :order-by (expression :asc #'name-of))

(let* ((cat (make-root 'cat))
       (name (make-alias cat #'name-of)))
  (make-query cat
	      :select name
	      :order-by (asc name)
	      :where (expression :betweenp
				 (slot-of cat #'age-of) 2 8)))

var cats =
    session.QueryOver<Cat>()
        .Where(c => c.Name == "Max")
        .And(c => c.Age > 4)
        .List();

(db-query ((cat cat))
    ((:where
      (:eq (name-of cat) "Max")
      (:> (age-of cat) 4))))

(let ((cat (make-root 'cat)))
  (make-query cat
	      :where (list
		      (expression := (slot-of cat #'name-of) "Max")
		      (expression :> (slot-of cat #'age-of) 2 8))))
Associations

IQueryOver<Cat,Kitten> catQuery =
    session.QueryOver<Cat>()
        .JoinQueryOver(c => c.Kittens)
            .Where(k => k.Name == "Tiddles");

(db-query ((cat cat)
	   (kitten (kittens-of cat)))
    ((:where (:eq (name-of kitten) "Tiddles"))))

(let* ((cat (make-root 'cat))
       (kitten (join-association cat #'kittens-of)))
  (make-query (list cat kitten)
	      :where (expression := (slot-of #'name-of) "Tiddles")))

=> (list cat kitten)

(db-query ((cat cat)
	   (kitten (kittens-of cat)))
    ((:where (:eq (name-of kitten) "Tiddles")))
  cat)

(let* ((cat (make-root 'cat))
       (kitten (join-association cat #'kittens-of)))
  (make-query cat :where (expression := (slot-of #'name-of)
				     "Tiddles")))

=> cats

Aliases

Cat catAlias = null;
Kitten kittenAlias = null;

IQueryOver<Cat,Cat> catQuery =
    session.QueryOver<Cat>(() => catAlias)
        .JoinAlias(() => catAlias.Kittens, () => kittenAlias)
        .Where(() => catAlias.Age > 5)
        .And(() => kittenAlias.Name == "Tiddles");

(db-query ((cat cat)
	   (kitten (kittens-of cat)))
    ((:where
      (:> (age-of cat) 5)
      (:eq (name-of kitten) "Tiddles")))
  cat)

(let* ((cat (make-root 'cat))
       (kitten (join-association cat #'kitten-of)))
  (make-query cat :where (list
			  (expression := (slot-of cat #'age-of) 5)
			  (expression := (slot-of kitten #'name-of)
				      "Tiddles"))))

Projections

IList selection =
    session.QueryOver<Cat>()
        .Select(
            c => c.Name,
            c => c.Age)
        .List<object[]>();

(db-query ((cat cat))
    ()
  (name-of cat)
  (age-of cat))

(let ((cat (make-root 'cat)))
  (make-query (list (slot-of cat #'name-of)
		    (slot-of cat #'age-of))))

IList selection =
    session.QueryOver<Cat>()
        .Select(Projections.ProjectionList()
            .Add(Projections.Property<Cat>(c => c.Name))
            .Add(Projections.Avg<Cat>(c => c.Age)))
        .List<object[]>();

(db-query ((cat cat))
    ()
  (name-of cat)
  (:avg #'age-of cat))

(query 'cat :select (list #'name-of (expression :avg #'age-of)))

(let* ((cat (make-root 'cat)))
  (make-query (list (slot-of #'name-of cats)
		    (expression :avg (slot-of cat #'age-of)))))

Subqueries

QueryOver<Cat> maximumAge =
    QueryOver.Of<Cat>()
        .SelectList(p => p.SelectMax(c => c.Age));

IList<Cat> oldestCats =
    session.QueryOver<Cat>()
        .WithSubquery.WhereProperty(c => c.Age).Eq(maximumAge)
        .List();

(db-query ((cat cat)
	   (maximum-age-cat cat))
    ((:having
      (:= (age-of cat)
	  (:max (age-of maximum-age-cat)))))
  cat)

(let* ((cat (make-root 'cat))
       (maximum-age-cat (make-root 'cat)))
  (make-query cat :where
	      (expression := (slot-of cat #'age-of)
			  (make-subquery
			   (expression :max (slot-of maximum-age-cat
						     #'age-of))))))

or without subquery

(let* ((cat (make-root 'cat))
       (maximum-age-cat (make-root 'cat)))
  (make-query cat :having
	      (expression := (slot-of cat #'age-of)
			  (expression :max
				      (slot-of maximum-age-cat
						#'age-of)))))

Limit, offset

(db-query ((cat cat)
	   (maximum-age-cat cat))
    ((:having
      (:eq (age-of cat)
	   (:max (age-of maximum-age-cat))))
     (:limit 10)
     (:offset 100))
  cat)

(make-query
 (make-root 'cat) :limit 100 :offset 100))

Fetching

(db-query ((cat cat))
    ((:fetch (cat #'kittens-of))))

(db-query ((cat (cat 35))
	   (kitten (kittens-of cat)))
    ((:fetch (cat #'kittens-of)
	     (kitten #'parents-of)))
  cat)

(query 'cat
       :fetch #'kittens-of
       :join (join-association #'kittens-of
			       :fetch #'parents-of))

Single instance

(db-query ((cat cat))
    ((:where (:eq (id-of cat) 35))
     (:single t)))

Проблема рекурсивных ключей. Идентификатор объекта не может идентифицироваться в дереве своим родителем.
В противном случае нуобходимо рекурсиное построение запроса.

If PK (user id and project id)

(db-query ((project-manager project-manager)
	   (project (project-of project-manager))
	   (user (user-of project-manager)))
    ((:single t)
     (:where
      (:eq (id-of user) 1)
      (:eq (id-of project) 1)))
  project-manager)

;; select roots only?

(db-query ((project-manager project-manager)
	   (project (project-of project-manager))
	   (user (user-of project-manager)))
  (:select project-manager)
  (:single t)
  (:where
   (:eq (id-of user) 1)
   (:eq (id-of project) 1))))

(let* ((project-manager (make-root 'project-manager))
       (project (join-association project-manager #'project-of))
       (user (join-association project-manager #'user-of)))
  (make-query (list project-manager project user)
	      

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

----------------

(let ((cat (make-root 'cat)))
  (make-query cat :single t
	      :fetch (fetch cat #'kittens)
	      :where (expression := (slot-of cat #'id-of) 23)))

----------------

(make-query 'cat
	    
	    :select (list #'name-of #'age-of)
	    :where (expression := #'name-of "Max")
	    :having (expression :< #'age-of (expression :avg #'age-of))

;; Criteria API

ICriteria crit = sess.CreateCriteria(typeof(Cat));
crit.SetMaxResults(50);
List cats = crit.List();

(db-list (make-query 'cat :limit 50))

IList cats = sess.CreateCriteria(typeof(Cat))
    .Add(Expression.Like("Name", "Fritz%"))
    .Add(Expression.Between("Weight", minWeight, maxWeight))
    .List();

(db-list
 (make-query 'cat :where (list
			  (expression :like #'name-of "Fritz%")
			  (expression :between #'weignt-of
				      min-weight max-weight))))

IList cats = sess.CreateCriteria(typeof(Cat))
    .Add(Expression.Like("Name", "Fritz%"))
    .Add(Expression.Or(
        Expression.Eq("Age", 0),
        Expression.IsNull("Age"))).List();

(db-list
 (make-query 'cat :where (list
			  (expression :like #'name-of "Fritz%")
			  (expression :or
				      (expression :eq #'age-of 0)
				      (expression :is-null #'age-of)))))

IList cats = sess.CreateCriteria(typeof(Cat))
    .Add( Expression.In( "Name", new String[] { "Fritz", "Izi", "Pk" } ) )
    .Add( Expression.Disjunction()
        .Add( Expression.IsNull("Age") )
    	.Add( Expression.Eq("Age", 0 ) )
    	.Add( Expression.Eq("Age", 1 ) )
    	.Add( Expression.Eq("Age", 2 ) )
    ) )
    .List();

(db-list
 (make-query 'cat :where (list
			  (expression :in #'name-of "Fritz", "Izi", "Pk")
			  (expression :or
				      (expression 'eq #'age-of 0)
				      (expression 'null #'age-of)))))


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


;;;;

Form:

(db-query ((cat cat)
	   (kitten (kittens-of cat)))
  (:select cat kitten
	   (- (age-of cat)
	      (age-of kitten))))

Expand into:

(let ((cat (make-root 'cat))
      (kitten (join-reference cat #'kittens-of)))
  (make-query
   (list cat kitten
	 (expression :-
		     (property cat #'age-of)
		     (property kitten #'age-of)))))
		     

(let ((project-mapping
       (map-class 'project (list "id")
		  (map-value 'id (column "id" "uuid"))))
      (user-mapping
       (map-class 'user (list "id")
		  (map-value 'id (column "id" "uuid"))))
      (project-member-mapping
       (map-subclass 'project-member (list "user_id" "prject_id")
		  (map-value