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

;; сделать db-read макросом? или оставить как функцию, а переделывать
;; выражение в with-session?  осталось написать

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

(db-query cat) => list all cats

(db-read 'cat)

=>

(let ((cat (bind-root 'cat)))
  (make-query cat))


OR

(with-session ((:mapping-schema mapping-name)
	       (:database-interface session-config))
  (db-read 'cat))

OR

(with-session ((:mapping-schema mapping-name)
	       (:database-interface session-config))
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

OR

(db-read 'cat :where
	 #'(lambda (root)
	     (expression #'eq (property-of root #'name-of) "Max")))

OR

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

(db-read 'cat
	 :select #'(lambda (cat)
		     (property-of cat #'name-of))
	 :where #'(lambda (cat)
		    (expression :between
				(property-of cat #'age-of) 2 8))
	 :order-by #'(lambda (cat)
		       (cons (property-of cat #'name-of) :ascending)))

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

(db-read 'cat
	 :where #'(lambda (root)
		    (list
		     (expression #'eq (property-of root #'name-of) "Max")
		     (expression #'> (property-of root #'age-of) 8))))

Associations

IQueryOver<Cat,Kitten> catQuery =
    session.QueryOver<Cat>()
        .JoinQueryOver(c => c.Kittens)
            .Where(k => k.Name == "Tiddles");

(db-query ((cat cat)
	   (kitten (kittens-of cat)))
    ((:where (:eq (name-of kitten) "Tiddles"))))

(db-read 'cat :join
	 #'(lambda (cat)
	     (reference-of cat #'kittens-of
			   :where #'(lambda (kitten)
				      (property-of kitten #'name-of
						   "Tiddles")))))

(let* ((cat (make-root 'cat))
       (kitten (join-association cat #'kittens-of)))
  (make-query (list cat kitten)
	      :where (expression := (slot-of #'name-of) "Tiddles")))

=> (list cat kitten)

(db-read 'cat :join
	 #'(lambda (cat)
	     (reference-of root #'kittens-of
			   :select #'(lambda (kitten)
				       kitten)
			   :where #'(lambda (kitten)
				      (property-of kitten
						   #'name-of
						   "Tiddles")))))

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

(db-read 'cat :having
	 #'(lambda (cat)
	     (let ((age-of-property (property-of cat #'age-of)))
	       (expression #'eq age-of-property
			   (expression #'max age-of-property)))))

or without subquery

(let* ((cat (make-root 'cat))
       (maximum-age-cat (make-root 'cat)))
  (make-query cat :having
	      (expression := (slot-of cat #'age-of)
			  (expression :max
				      (slot-of maximum-age-cat
						#'age-of)))))

(db-read '(cat cat)
	 :where #'(lambda (cat maximum-age-cat)
		    (expression #'= 

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

(db-read 'cat :limit 100 :offset 100)

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

(db-read 'cat :fetch #'(lambda (cat)
			 (reference-of cat #'kittens-of)))

Single instance

(db-query ((cat cat))
    ((:where (:eq (id-of cat) 35))
     (:single t)))

(db-read 'cat
	 :single t
	 :where (lambda (cat)
		  (expression #'eq (property-of cat #'id-of) 35)))

;; Проблема рекурсивных ключей. Идентификатор объекта не может
;; идентифицироваться в дереве своим родителем. В противном случае
;; необходимо рекурсиное построение запроса.

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

;;;;;;;;;;;;;;;;;;;;;;;;;

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


	 



		   
		    
	 (
    
    ( (make-loaders e-loader select-list)))
    (

=> make-loaders

(defun sql-query (query)
  ("SELECT"
   (select-list-of query)
   "FROM"
   (tquery-mappings-of query)




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


(let ((cat (bind-root 'cat))
      (kittens (bind-reference cat #'kittens-of)))
  (make-query (fetch-also cat kittens)))

(let ((curriculum
       (bind-root 'curriculum))
      (subjects
       (bind-reference curriculum #'subjects-of)))
  (make-query (join-fetch curriculum
			  (join-fetch subjects #'semester-works-of))))

(db-read 'curriculum
	 :select #'(lambda (curriculum)
		     (list
		      (property curriculum #'id-of)
		      (property curroculum #'name-of)
		      (expression #'count (property curriculum #'version-of))))
	 :join #'(lambda (curriculum)
		   (join (list
			  (reference curriculum #'semesters-of)
			  (reference curriculum #'subject-courses-of))
			 :join #'(lambda (semester subject-course)
				   (join
				    (list
				     (reference semester #'subject-parts-of)
				     (reference subject-course #'course-parts-of))))))
	 :where #'(lambda (curriculum)
		    (expression #'eq
				(reference curriculum #'direction-of)
				direction))
	 :fetch #'(lambda (curriculum)
		    (fetch #'(lambda (subject-course semester)
			       (values
				(reference semester #'subject-parts-of)
				(reference subject-course #'course-parts-of)))
			   (reference curriculum #'subject-courses-of)
			   (reference curriculum #'semesters-of))))

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
    deserializer serializer))
    
(?- (or (class-mapping class-name ?table-name)
	(iheritance class-name ?superclass-name ?foreign-key))

    
    (

;; without refernces
'((test-class-1
   ("test-table-1" "id"))
  (test-class-2
   ("test-table-2" "id"))
  (test-class-3
   ("test-table-3" "id")
   ((test-class-1
     ("test-table-1" "id"))
    "test-class-1-id")
   ((test-class-2
     ("test-table-2" "id"))
    "test-class-2-id"))
  (test-class-4
   ("test-table-4" "id")
   ((test-class-3
     ("test-table-3" "id"))
    ((test-class-1
      ("test-table-1" "id"))
     "test-class-1-id")
    ((test-class-2
      ("test-table-2" "id"))
     "test-class-2-id")
    "id")))
'((test-class-1
   (value-slot-1 "value-1")
   (value-slot-1 "value-2"))
  (test-class-2)
  (test-class-3)
  (test-class-4))

;; without refernces 2
'(((test-class-1
    ("test-table-1" "id"))
   (value-slot-1 "value-1")
   (value-slot-2 "value-2"))
  ((test-class-2
    ("test-table-2" "id")))
  ((test-class-3
    ("test-table-3" "id")
    (((test-class-1
       ("test-table-1" "id"))
      (value-slot-1 "value-1")
      (value-slot-2 "value-2"))
     "test-class-1-id")
    (((test-class-2
       ("test-table-2" "id")))
     "test-class-2-id"))
   (value-slot-3 "value-3")
   (value-slot-4 "value-4"))
  ((test-class-4
    ("test-table-4" "id")
    (((test-class-3
       ("test-table-3" "id")
       (((test-class-1
	  ("test-table-1" "id"))
	 (value-slot-1 "value-1")
	 (value-slot-2 "value-2"))
	"test-class-1-id")
       (((test-class-2
	  ("test-table-2" "id")))
	"test-class-2-id")))
     "id"))
   (value-slot-5 "value-5")
   (value-slot-6 "value-6")))

;; with references
'((((test-class-1
     ("test-table-1" "id")
     (value-slot-1 "value-1")
     (value-slot-1 "value-2"))
    (one-to-many-slot-1
     (test-class-3
      ("test-table-3" "id")
      ((test-class-1
	("test-table-1" "id" ("id")))
       "test-class-1-id")
      ((test-class-2
	("test-table-2" "id" ("id")))
       "test-class-2-id")) "test-class-3-id"))
   (many-to-one-slot-1
    (test-class-4
     ("test-table-4" "id")
     (test-class-3
      ("test-table-3" "id")
      ((test-class-1
	("test-table-1" "id"))
       "test-class-1-id")
      ((test-class-2
	("test-table-2" "id"))
       "test-class-2-id"))
     "id") "test-class-4-id"))
  (test-class-2
   ("test-table-2" "id"))
  (test-class-3
   ("test-table-3" "id")
   ((test-class-1
     ("test-table-1" "id"))
    "test-class-1-id")
   ((test-class-2
     ("test-table-2" "id"))
    "test-class-2-id"))
  (test-class-4
   ("test-table-4" "id")
   ((test-class-3
     ("test-table-3" "id"))
    ((test-class-1
      ("test-table-1" "id" ("id")))
     "test-class-1-id")
    ((test-class-2
      ("test-table-2" "id" ("id")))
     "test-class-2-id"))
   "id"))
    
'((test-class-1
  ("test-table-1" "id")
  (value-slot-1 "value-1")
  (value-slot-1 "value-2")))

;;(make-mapping-schema
;; #'(lambda (schema)
;;     (values
;;      (map-class 'project "projects"
;;		 :primary-key '("id")
;;		 :slots #'(lambda (

'(((test-class-1 ("test-table-1" "id"))
   (test-class-2 ("test-table-2" "id"))
   (test-class-3 ("test-table-3" "id")
    ((test-class-1 ("test-table-1" "id")) "test-class-1-id")
    ((test-class-2 ("test-table-2" "id")) "test-class-2-id"))
   (test-class-4 ("test-table-4" "id")
    (test-class-3 ("test-table-3" "id")
		  ((test-class-1 ("test-table-1" "id")) "test-class-1-id")
		  ((test-class-2 ("test-table-2" "id")) "test-class-2-id")))
   (test-class-5 ("test_table_5" "id")))
  ((test-class-1
    (value-slot-1 "value-1")
    (value-slot-1 "value-2"))
   (test-class-2)
   (test-class-3
    (value-slot-1 "value-1" test-class-1)
    (value-slot-1 "value-2" test-class-1))
   (test-class-4
    ((value-slot-1 "value-1") test-class-1)
    ((value-slot-1 "value-2") test-class-1)))
  ((test-class-5
    (slot-1 (:many-to-one test-class-4 "test_class_4_id"))
    (slot-2 (:one-to-many test-class-3 "test_class_4_id")))))

  
((test-class-1
    (value-slot-1 "value-1")
    (value-slot-1 "value-2"))
 (test-class-2
  (many-to-one-slot :many-to-one
		    (test-class-4
		     "test-table-4"
		     (test-class-3
		      "test-table-3"
		      ((test-class-1 "test-table-1")
		       "test-class-1-id")
		      ((test-class-2 "test-table-2")
		       "test-class-2-id")))))
   (test-class-3)
   (test-class-4)))

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

(define-schema test-mapping ()
  (test-class
   (("test-table" "id"))
   ((id "id" "integer")))
  (test-class-2
   (("test_table_2" "id"))
   ((id "id" "integer")))
  (test-class-3
   (("test_table_3" "test_class_1_id" "test_class_2_id")
    (test-class-1 "test_class_1_id")
    (test-class-2 "test_class_2_id")))
  (test-class-4
   (("test_table_4" "test_class_1_id")
    (test-class-3 "test_class_1_id" "test_class_2_id")))
  (test-class-5
   (("test_table_5" "id"))
   ((id "id" "integer"))))

(define-schema projects-managment ()
  (user
   (("users" "id"))
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
  (project-participation
   (("project_memebers" "project_id" "user_id"))
   (project (:many-to-one project "project_id"))
   (user (:many-to-one user "user_id")))
  (project-managment
   (("project_managers" "project_id" "user_id")
    (project-participation "project_id" "user_id"))
   (project-participation
    (:many-to-one project-participation "project_id" "user_id")))
  (project
   (("projects" "id"))
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
		    #'alexandria:hash-table-values)))

(defun compute-value-columns (&rest class-mappings)
  (mapcar #'(lambda (mapping)
	      (destructuring-bind (class-name
				   ((table-name &rest primary-key)
				    &rest superclasses)
				   &rest slots)
		  mapping
		(list table-name
		      (mapcar #'(lambda (value-mapping)
				  (destructuring-bind
					(slot-name
					 (mapping-type &rest columns)
					 &optional serialize-fn
					 deserialize-fn)
				      value-mapping
				    columns))
			      (remove-if-not #'(lambda (slot-mapping)
						 (destructuring-bind
						       (slot-name
							(mapping-type &rest columns)
							&optional serialize-fn
							deserialize-fn)
						     slot-mapping
						   (eq mapping-type :value)))
					     slots)))))
	  class-mappings))

(defun compute-many-to-one-columns (tables &rest class-mappings)
  (mapcar #'(lambda (mapping)
	      (destructuring-bind (class-name
				   ((table-name &rest primary-key)
				    &rest superclasses)
				   &rest slots)
		  mapping
		(list table-name
		      (mapcar #'(lambda (many-to-one-mapping)
				  (destructuring-bind
					(slot-name
					 (mapping-type reference-class
						       &rest columns)
					 &optional serialize-fn
					 deserialize-fn)
				      (mapcar value-mapping
				    columns))
			      (remove-if-not #'(lambda (slot-mapping)
						 (destructuring-bind
						       (slot-name
							(mapping-type reference-class
								      &rest columns)
							&optional serialize-fn
							deserialize-fn)
						     slot-mapping
						   (eq mapping-type :many-to-one)))
					     slots)))))
	  class-mappings))



(defun mapping-type (mapping)
  (destructuring-bind
	(slot-name (mapping-type columns &rest columns)
		   &optional deserialize-fn serialize-fn)
      mapping
    (declare (ignore columns columns deserialize-fn serialize-fn))
    mapping-type))

(defun map-properties (function &rest slot-mappings)
  (mapcar #'(lambda (mapping)
	      (destructuring-bind
		    (slot-name (mapping-type columns &rest columns)
			       &optional deserialize-fn serialize-fn)
		  mapping
		(declare (ignore mapping-type))
		(funcall function
			 :slot-name slot-name
			 :serialize-fn deserialize-fn
			 :deserialize-fn deserialize-fn
			 :columns (list* column columns))))
	  (remove :property slot-mappings
		  :key #'mapping-type
		  :test-not #'eq)))

(defun map-many-to-one (function &rest slot-mappings)
  (mapcar #'(lambda (mapping)
	      (destructuring-bind
		    (slot-name
		     (mapping-type class-name columns &rest columns)
		     &optional deserialize-fn serialize-fn)
		  mapping
		(declare (ignore mapping-type))
		(funcall function
			 :slot-name slot-name
			 :serialize-fn deserialize-fn
			 :deserialize-fn deserialize-fn
			 :columns (list* column columns)
			 :class (find-class class-name))))
	  (remove :many-to-one slot-mappings
		  :key #'mapping-type
		  :test-not #'eq)))

(defun map-one-to-many (function &rest slot-mappings)
  (mapcar #'(lambda (mapping)
	      (destructuring-bind
		    (slot-name
		     (mapping-type class-name columns &rest columns)
		     &optional deserialize-fn serialize-fn)
		  mapping
		(declare (ignore mapping-type))
		(funcall function
			 :slot-name slot-name
			 :serialize-fn deserialize-fn
			 :deserialize-fn deserialize-fn
			 :columns (list* column columns)
			 :class (find-class class-name))))
	  (remove :one-to-many slot-mappings
		  :key #'mapping-type
		  :test-not #'eq)))


  
  ;;		   (destructuring-bind (class-name
;;					((table-name &rest primary-key)
;;					 &rest superclasses)
;;					&rest slots)
;;		       mapping
;;		     (list class-name table-name)))
;;	       mappings)
;;      ,(apply #'compute-value-columns mappings))))
      

(define-schema test-schema ()
  ("test_table_1"
   ((test-class-1 "id"))
   (id (:property ("id" "integer"))))
  ("test_table_2"
   ((test-class-2 "id"))
   (id (:property ("id" "integer"))))
  ("test_table_3"
   ((test-class-3 "test_class_1_id" "test_class_2_id")
    (test-class-1 "test_class_1_id")
    (test-class-2 "test_class_2_id")))
  ("test_table_4"
   ((test-class-4 "test_class_1_id")
    (test-class-3 "test_class_1_id" "test_class_2_id")))
  ("test_table_5"
   ((test-class-5  "id"))
   (id (:property ("id" "integer")))))

(define-schema projects-managment ()
  (user
   (("users" "id"))
   (id (:property ("id" "uuid")))
   (name (:property ("name" "varchar")))
   (login (:property ("login" "varchar")))
   (password (:property ("password" "varchar")))
   (project-managments
    (:one-to-many project-managment "user_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (project-of role) role))
		 roles)))
    #'alexandria:hash-table-values)
   (project-participations
    (:one-to-many project-managment "user_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (project-of role) role))
		 roles)))
    #'alexandria:hash-table-values))
  (project-participation
   (("project_memebers" "project_id" "user_id"))
   (project (:many-to-one project "project_id"))
   (user (:many-to-one user "user_id")))
  (project-managment
   (("project_managers" "project_id" "user_id")
    (project-participation "project_id" "user_id")))
  (project
   (("projects" "id"))
   (id (:property ("id" "uuid")))
   (name (:property ("name" "varchar")))
   (begin-date (:property ("begin_date" "date")))
   (objects
    (:many-to-one project-root-object "project_id"))
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
   (project-members
    (:one-to-many project-member "project_id")
    #'(lambda (&rest roles)
	(alexandria:alist-hash-table
	 (mapcar #'(lambda (role)
		     (cons (user-of role) role))
		 roles)))
    #'alexandria:hash-table-values)))



(defclass class-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (table-name :initarg :class-name :reader table-name-of)
   (primary-key :initarg :foreign-key :reader foreign-key)))

(defclass superclass-mapping ()
  ((class-name :initarg :class-name :reader class-name-of)
   (superclass-name :initarg :superclass-name :reader superclass-name-of)
   (foreign-key :initarg :foreign-key :reader foreign-key)))

(defclass slot-mapping ()
  ((class-name :initarg :class-name
	       :reader class-name-of)
   (slot-name :initarg :class-name
	      :reader slot-name-of)))

(defclass value-mapping (slot-mapping)
  ((columns :initarg :columns
	    :reader columns-of)
   (serializer :initarg :serializer
	       :reader serializer-of)
   (deserializer :initarg :deserializer
		 :reader deserializer-of)))

(defclass reference-mapping (slot-mapping)
  ((referenced-class-name :initarg :referenced-class-name
			  :reader referenced-class-name-of)
   (foreign-key :initarg :foreign-key
		:reader foreign-key-of)))

(defclass many-to-one-mapping (reference-mapping)
  ())

(defclass one-to-many-mapping (reference-mapping)
  ((serializer :initarg :serializer
	       :reader serializer-of)
   (deserializer :initarg :deserializer
		 :reader deserializer-of)))

(defvar *superclass-primary-key*)
(defvar *subclass-alias*)

(define-query select (from-clause)
  (root
   (let ((alias (make-alias)))
     (lisp*
      (list :from table-name :as alias)
      (let ((*subclass-alias* alias)
	    (*superclass-primary-key*
	     (apply #'append-alias alias primary-key)))
	(append superclasses subclasses))
  (subclasses
   (let ((alias (make-alias)))
     (lisp*
      (list :left-join table-name :as alias
	    :on (pairlis *superclass-primary-key*
			 (apply #'append-alias alias foreign-key)))
      (let ((*subclass-alias* alias)
	    (*superclass-primary-key*
	     (apply #'append-alias alias primary-key)))
	(append
	 (superclasses)
	 (subclasses))))))
  (superclasses
   (let ((alias (make-alias)))
     (lisp*
      (list :inner-join table-name :as alias
	    :on (pairlis
		 (apply #'append-alias alias foreign-key)
		 (apply #'append-alias *subclass-alias* primary-key)))
      (let ((*subclass-alias* alias)
	    (*superclass-primary-key*
	     (apply #'append-alias alias primary-key)))
	(append
	 (superclasses)
	 (subclasses)))))))

(defun select (class-mapping)
  (destructuring-bind (table-name)
  #'(lambda (alias-fn)
      (values
       (:from table-name :as (funcall alias-fn))))))


  
(define-query select (select-list from-clause)
  (root
   ((*subclass-alias* (make-alias))
    ((table-name primary-key)
    
   
   (superclass-mappings subclass-mappings))
  (superclass-mappings
   ((*subclass-alias* (make-alias)))
   (superclass-mapping superclass-mappings))
  (subclass-mappings
   ((*subclass-alias* (make-alias)))
   (subclass-mapping subclass-mappings))
  (superclass-mapping
   ())
  (subclass-mapping))
  
   (let ((alias (make-alias)))
     
     (lisp*
      (list :from table-name :as alias)
      (let ((*subclass-alias* alias)
	    (*superclass-primary-key*
	     (apply #'append-alias alias primary-key)))
	(append superclasses subclasses))
  (subclasses
   (let ((alias (make-alias)))
     (lisp*
      (list :left-join table-name :as alias
	    :on (pairlis *superclass-primary-key*
			 (apply #'append-alias alias foreign-key)))
      (let ((*subclass-alias* alias)
	    (*superclass-primary-key*
	     (apply #'append-alias alias primary-key)))
	(append
	 (superclasses)
	 (subclasses))))))
  (superclasses
   (let ((alias (make-alias)))
     (lisp*
      (list :inner-join table-name :as alias
	    :on (pairlis
		 (apply #'append-alias alias foreign-key)
		 (apply #'append-alias *subclass-alias* primary-key)))
      (let ((*subclass-alias* alias)
	    (*superclass-primary-key*
	     (apply #'append-alias alias primary-key)))
	(append
	 (superclasses)
	 (subclasses)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))

(defun select (arg &rest args)
  (optima:match arg
    ((property :class-name class-name))
    ()))

(define-query select
    ((select-list (list))
     (from-clause (list))
     (unique-table-index 0))
  ((:class-name class-name) ;; (property :class-name class-name)
   ((:select-list select-list)
    (:from-clause from-clause)
    (:alias (make-alia))))
  ((:table-name table-name)
   ((list* (list :from ) from-clasuse  format from-clause "FROM ")))
  ((:table-name table-name)
   #'(lambda (&key alias &allow-other-keys)
       (format from-clause "~a AS ~a~% " table-name)))
  ((:primary-key primary-key))
  ((:superclass-name superclass-name)
   (:subclass-alias alias)
   (:alias (make-alias))
   #'(lambda (&key &allow-other-keys)
       (format from-clause "INNER JOIN ")))
  ((:superclass-foreign-key foreign-key)))


Structure of a Query

Simple Expressions

(with-session ((:mapping-schema mapping-name)
	       (:database-interface session-config))
  (db-read 'cat))

IList<Cat> cats =
    session.QueryOver<Cat>()
        .Where(c => c.Name == "Max")

(db-read 'cat
	 :where #'(lambda (cat)
		    (expression-eq cat #'name-of "Max")))

Additional Restrictions

var catNames = session.QueryOver<Cat>()
        .WhereRestrictionOn(c => c.Age).IsBetween(2).And(8)
        .Select(c => c.Name)
        .OrderBy(c => c.Name).Asc
        .List<string>();

(db-read 'cat
	 :select #'(lambda (cat)
		     (list (property-of cat #'name-of)))
	 :where #'(lambda (cat)
		    (expression-between cat #'age-of 2 8))
	 :order-by #'(lambda (cat)
		       (ascending cat #'name-of)))

var cats =
    session.QueryOver<Cat>()
        .Where(c => c.Name == "Max")
        .And(c => c.Age > 4)
        .List();

(db-read 'cat
	 :where #'(lambda (cat)
		    (list
		     (expression-more root #'age-of 8)
		     (expression-eq root #'name-of "Max"))))

Associations

IQueryOver<Cat,Kitten> catQuery =
    session.QueryOver<Cat>()
        .JoinQueryOver(c => c.Kittens)
            .Where(k => k.Name == "Tiddles");

(db-read 'cat
	 :alias #'(lambda (cat)
		    (alias cat :cat))
	 :join #'(lambda (&key cat)
		   (join-reference cat #'kittens-of :kitten))
	 :where #'(lambda (&key kitten &allow-other-keys) ;; cat
		    (expression-eq kitten #'name-of "Tiddles")))

Projections

IList selection =
    session.QueryOver<Cat>()
        .Select(
            c => c.Name,
            c => c.Age)
        .List<object[]>();

(db-read 'cat :select #'(lambda (cat)
			  (list
			   (property-of cat #'age-of)
			   (property-of cat #'name-of))))

IList selection =
    session.QueryOver<Cat>()
        .Select(Projections.ProjectionList()
            .Add(Projections.Property<Cat>(c => c.Name))
            .Add(Projections.Avg<Cat>(c => c.Age)))
        .List<object[]>();

(db-read 'cat :select #'(lambda (cat)
			  (list
			   (property-of cat #'name-of)
			   (projection-avg cat #'age-of))))

Subqueries (no subqueries)

QueryOver<Cat> maximumAge =
    QueryOver.Of<Cat>()
        .SelectList(p => p.SelectMax(c => c.Age));

IList<Cat> oldestCats =
    session.QueryOver<Cat>()
        .WithSubquery.WhereProperty(c => c.Age).Eq(maximumAge)
        .List();

(db-read 'cat :having #'(lambda (cat)
			  (property-more-than cat #'age-of
					      (projection-max cat #'age-of))))

Limit, offset

(db-read 'cat :limit 100 :offset 100)

Fetching

(db-read 'cat
	 :fetch #'(lambda (cat)
		    (fetch cat #'kittens-of)))

Single instance

(db-read 'cat :single t
	 :where (lambda (cat)
		  (expression-eq cat #'id-of 35)))
