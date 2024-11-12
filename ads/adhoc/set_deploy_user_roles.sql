-- Create the deploy read/write user that is used by ingest and to sync schemas in Data Studio 
DROP USER IF EXISTS [deploy]
CREATE USER [deploy] FROM login [deploy];
EXEC sp_addRoleMember 'db_ddladmin', 'deploy';
EXEC sp_addRoleMember 'db_datawriter', 'deploy';
EXEC sp_addRoleMember 'db_datareader', 'deploy';
GRANT VIEW DEFINITION TO [deploy];
GO