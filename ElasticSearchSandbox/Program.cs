using Elasticsearch.Net;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSearchSandbox
{
	class Program
	{
		static void Main(string[] args) => MainAsync(args).GetAwaiter().GetResult();
		static async Task MainAsync(string[] args)
		{
			try
			{
				var esClient = new ElasticLowLevelClient(ElasticSearchConnectionSettings.LocalSimple.GetConfiguration());
				var nestClient = new ElasticClient(ElasticSearchConnectionSettings.LocalSimple.GetNestConfiguration());

				var mlesClient = new ElasticSearchClient("http://localhost:9200/companydatabase/_search");

				return;

				var esTest = await esClient.PingAsync<StringResponse>();
				var nestTest = await nestClient.PingAsync();

				var indices = await GetIndices(nestClient);
				ShowResults(indices);

				var repos = await GetRepositories(nestClient);
				ShowResults(repos);

				var master = await GetMaster(nestClient);
				ShowResults(master);

				var aliases = await GetAliases(nestClient);
				ShowResults(aliases);
			}
			catch (Exception ex)
			{
				Console.WriteLine("FATAL ERROR: " + ex.Message);
			}
			Console.WriteLine("Press any key to exit.");
			Console.ReadKey();
		}

		static async Task<IEnumerable<CatIndicesRecord>> GetIndices(ElasticClient client)
		{
			var result = await client.CatIndicesAsync();
			if (!result.IsValid)
				throw new Exception("Result Invalid: " + result.OriginalException.Message);
			return result.Records;
		}
		private static void ShowResults(IEnumerable<CatIndicesRecord> indices)
		{
			Console.WriteLine($"{indices.Count()} Indices Found");
			foreach (var cat in indices)
			{
				Console.WriteLine($"{cat.Index} ({cat.DocsCount})");
			}
		}

		static async Task<IEnumerable<CatRepositoriesRecord>> GetRepositories(ElasticClient client)
		{
			var result = await client.CatRepositoriesAsync();
			if (!result.IsValid)
				throw new Exception("Result Invalid: " + result.OriginalException.Message);
			return result.Records;
		}
		private static void ShowResults(IEnumerable<CatRepositoriesRecord> repos)
		{
			Console.WriteLine($"{repos.Count()} Repositories Found");
			foreach (var cat in repos)
			{
				Console.WriteLine($"{cat.Id} ({cat.Type})");
			}
		}

		static async Task<IEnumerable<CatMasterRecord>> GetMaster(ElasticClient client)
		{
			var result = await client.CatMasterAsync();
			if (!result.IsValid)
				throw new Exception("Result Invalid: " + result.OriginalException.Message);
			return result.Records;
		}
		private static void ShowResults(IEnumerable<CatMasterRecord> master)
		{
			Console.WriteLine($"{master.Count()} Master Fields Found");
			foreach (var cat in master)
			{
				Console.WriteLine($"{cat.Node} ({cat.Id})");
			}
		}

		static async Task<IEnumerable<CatAliasesRecord>> GetAliases(ElasticClient client)
		{
			var result = await client.CatAliasesAsync();
			if (!result.IsValid)
				throw new Exception("Result Invalid: " + result.OriginalException.Message);
			return result.Records;
		}
		private static void ShowResults(IEnumerable<CatAliasesRecord> aliases)
		{
			Console.WriteLine($"{aliases.Count()} Aliases Found");
			foreach (var alias in aliases)
			{
				Console.WriteLine($"{alias.Alias} ({alias.Index} | ({alias.SearchRouting})");
			}
		}

	}
	public class ElasticSearchConnectionSettings
	{
		public ElasticSearchConnectionSettings(string connectionUri)
		{
			ConnectionUris.Add(connectionUri);
		}
		public static ElasticSearchConnectionSettings LocalSimple => new ElasticSearchConnectionSettings("http://localhost:9200");

		public List<string> ConnectionUris { get; set; } = new List<string>();
		public bool PooledConnections => ConnectionUris.Count > 1;

		public IConnectionConfigurationValues GetConfiguration()
		{
			if (ConnectionUris == null || ConnectionUris.Count < 1)
			{
				throw new Exception("At least one valid connection URI is required.");
			}
			if (PooledConnections)
			{
				return new ConnectionConfiguration(new SniffingConnectionPool(ConnectionUris.Select(u => new Uri(u))));
			}
			else
			{
				return new ConnectionConfiguration(new Uri(ConnectionUris.First()));
			}
		}

		internal Nest.ConnectionSettings GetNestConfiguration()
		{
			if (ConnectionUris == null || ConnectionUris.Count < 1)
			{
				throw new Exception("At least one valid connection URI is required.");
			}
			if (PooledConnections)
			{
				return new Nest.ConnectionSettings(new SniffingConnectionPool(ConnectionUris.Select(u => new Uri(u))));
			}
			else
			{
				return new Nest.ConnectionSettings(new Uri(ConnectionUris.First()));
			}
		}
	}
	public class PersonPoco
	{
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public string Designation { get; set; }
		public decimal? Salary { get; set; }
		public DateTime? LastDateOfJoiningName { get; set; }
		public string Address { get; set; }
		public string Gender { get; set; }
		public int? Age { get; set; }
		public string MaritalStatus { get; set; }
		public string Interests { get; set; }
	}
}
