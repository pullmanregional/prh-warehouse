drop user if exists [deploy]
create user [deploy] from login [deploy];
exec sp_addRoleMember 'db_ddladmin', 'deploy';
exec sp_addRoleMember 'db_datawriter', 'deploy';
exec sp_addRoleMember 'db_datareader', 'deploy';
GRANT VIEW DEFINITION TO [deploy];
go