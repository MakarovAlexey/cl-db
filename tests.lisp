(defpackage #:cl-db.tests
  (:use #:cl #:cl-db #:lift))

(in-package #:cl-db.tests)

(deftestsuite test-mappings () ())

(addtest (test-mappings) direct-mapping-definition
  (make-persistence-unit (map-project)
			 (map-user)
			 (map-project-member)
			 (map-project-manager)))