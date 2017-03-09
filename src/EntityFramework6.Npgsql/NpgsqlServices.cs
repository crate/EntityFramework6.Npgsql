#region License
// The PostgreSQL License
//
// Copyright (C) 2016 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Text;
using JetBrains.Annotations;
#if ENTITIES6
using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Migrations.Sql;
using System.Data.Entity.Infrastructure.DependencyResolution;
#else
using System.Data.Common;
using System.Data.Common.CommandTrees;
using System.Data.Metadata.Edm;
#endif
using Npgsql.SqlGenerators;
using DbConnection = System.Data.Common.DbConnection;
using DbCommand = System.Data.Common.DbCommand;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Npgsql
{
#if ENTITIES6
    public class NpgsqlServices : DbProviderServices
#else
    internal class NpgsqlServices : DbProviderServices
#endif
    {
        public static NpgsqlServices Instance { get; } = new NpgsqlServices();

#if ENTITIES6
        public NpgsqlServices()
        {
            AddDependencyResolver(new SingletonDependencyResolver<Func<MigrationSqlGenerator>>(
                () => new NpgsqlMigrationSqlGenerator(), nameof(Npgsql)));
        }
#endif

        protected override DbCommandDefinition CreateDbCommandDefinition([NotNull] DbProviderManifest providerManifest, [NotNull] DbCommandTree commandTree)
            => CreateCommandDefinition(CreateDbCommand(((NpgsqlProviderManifest)providerManifest).Version, commandTree));

        internal DbCommand CreateDbCommand(Version serverVersion, DbCommandTree commandTree)
        {
            if (commandTree == null)
                throw new ArgumentNullException(nameof(commandTree));

            var command = new NpgsqlCommand();

            foreach (var parameter in commandTree.Parameters)
            {
                var dbParameter = new NpgsqlParameter
                {
                    ParameterName = parameter.Key,
                    NpgsqlDbType = NpgsqlProviderManifest.GetNpgsqlDbType(((PrimitiveType)parameter.Value.EdmType).PrimitiveTypeKind)
                };
                command.Parameters.Add(dbParameter);
            }

            TranslateCommandTree(serverVersion, commandTree, command);

            return command;
        }

        internal void TranslateCommandTree(Version serverVersion, DbCommandTree commandTree, DbCommand command, bool createParametersForNonSelect = true)
        {
            SqlBaseGenerator sqlGenerator;

            DbQueryCommandTree select;
            DbInsertCommandTree insert;
            DbUpdateCommandTree update;
            DbDeleteCommandTree delete;
            if ((select = commandTree as DbQueryCommandTree) != null)
                sqlGenerator = new SqlSelectGenerator(select);
            else if ((insert = commandTree as DbInsertCommandTree) != null)
                sqlGenerator = new SqlInsertGenerator(insert);
            else if ((update = commandTree as DbUpdateCommandTree) != null)
                sqlGenerator = new SqlUpdateGenerator(update);
            else if ((delete = commandTree as DbDeleteCommandTree) != null)
                sqlGenerator = new SqlDeleteGenerator(delete);
            else
            {
                // TODO: get a message (unsupported DbCommandTree type)
                throw new ArgumentException();
            }
            sqlGenerator.CreateParametersForConstants = select == null && createParametersForNonSelect;
            sqlGenerator.Command = (NpgsqlCommand)command;
            sqlGenerator.Version = serverVersion;

            sqlGenerator.BuildCommand(command);
        }

        protected override string GetDbProviderManifestToken([NotNull] DbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            var serverVersion = "";
            UsingPostgresDbConnection((NpgsqlConnection)connection, conn => {
                serverVersion = conn.ServerVersion;
            });
            return serverVersion;
        }

        protected override DbProviderManifest GetDbProviderManifest([NotNull] string versionHint)
        {
            if (versionHint == null)
                throw new ArgumentNullException(nameof(versionHint));
            return new NpgsqlProviderManifest(versionHint);
        }

#if ENTITIES6
        protected override bool DbDatabaseExists([NotNull] DbConnection connection, int? commandTimeout, [NotNull] StoreItemCollection storeItemCollection)
        {
            // Not supported in CrateDB
            return true;
        }

        protected override void DbCreateDatabase([NotNull] DbConnection connection, int? commandTimeout, [NotNull] StoreItemCollection storeItemCollection)
        {
            // Not supported in CrateDB
        }

        protected override void DbDeleteDatabase([NotNull] DbConnection connection, int? commandTimeout, [NotNull] StoreItemCollection storeItemCollection)
        {
            // Not supported in CrateDB
        }
#endif

        static void UsingPostgresDbConnection(NpgsqlConnection connection, Action<NpgsqlConnection> action)
        {
            var connectionBuilder = new NpgsqlConnectionStringBuilder(connection.ConnectionString)
            {
                Database = connection.Settings.EntityAdminDatabase ?? "template1",
                Pooling = false
            };

            using (var masterConnection = connection.CloneWith(connectionBuilder.ConnectionString))
            {
                masterConnection.Open();//using's Dispose will close it even if exception...
                action(masterConnection);
            }
        }
    }
}
