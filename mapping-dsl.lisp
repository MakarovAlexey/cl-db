;; implement (or class-name null) - type form
;; implement type declaration for values

(in-package #:cl-db)

;; прикрутить компиляцию схемы, взаимоувязать переопределение или
;; добавление нового отображения с пересозданием схемы

;;(defvar *class-mapping-definitions* (make-hash-table))

;; при определеении отображения класса, в пакете создается функция с
;; именем вида {class-name}-mapping, далее, при вызове with-session в
;; указанном пакете (по умолчанию в текущем) производится поиск всех
;; {class-name}-mapping и создание в нем же переменной с именем
;; указанным в переменной (*mapping-schema-symbol* "*mapping-schema*"
;; ???)

;; cl-db:default-mapping-schema - может ввести отображение по
;; умолчанию?  в этом не нужды, так как отображение можно определить в
;; отдельном пакете. Если реализовать экспорт символов отображения и
;; имопртровать их в нужный пакет? (:use в defpackage или use-mapping)

;; возможно имеет смысл регистрировать определения по пакетам, но не
;; интернировать символы (учитыать по пакетам и названиям классов)

;; 1) импортируемость (возможность определения нескольких схем
;; отображения) 2) возможность определения схемы внутри пакета

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
