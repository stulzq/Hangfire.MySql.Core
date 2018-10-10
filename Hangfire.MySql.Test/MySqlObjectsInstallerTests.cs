using System;
using Xunit;
using Hangfire.MySql;

namespace Hangfire.MySql.Test
{
	public class MySqlObjectsInstallerTests
	{
		[Fact(DisplayName = "GetInstallSql")]
		public void GetInstallSql()
		{
			Assert.NotNull(MySqlObjectsInstaller.GetStringResource("Install.sql"));
		}
	}
}
