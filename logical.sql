SELECT pg_create_logical_replication_slot(
  'organisationunit_slot',
  'pgoutput'
);