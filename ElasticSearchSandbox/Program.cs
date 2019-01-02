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
				var mlesClient = new ElasticSearchClient("http://localhost:9200/companydatabase/_search");
				var searchResponse = await mlesClient.SearchWithScroll();
				Console.WriteLine("ML ES Client Search Complete");
				Console.WriteLine($"{searchResponse.Results.Total} Total Hits");
				Console.WriteLine($"{searchResponse.Results.Hits.Count} Hits This Page");
				foreach(var hit in searchResponse.Results.Hits)
				{
					var poco = hit.ParseSource<PersonPoco>();
					if (poco == null)
						Console.WriteLine("No Source Result Available");
					else
						Console.WriteLine($"First Result {poco.FirstName} {poco.LastName}, {poco.Designation} ({poco.Age}yo {poco.Gender}) Started {poco.DateOfJoining:d}");
				}
				var scrollResponse = await mlesClient.Scroll(searchResponse);
				Console.WriteLine("ML ES Client Scroll Complete");
				Console.WriteLine($"{scrollResponse.Results.Total} Total Hits");
				Console.WriteLine($"{scrollResponse.Results.Hits.Count} Hits This Page");
				foreach(var hit in scrollResponse.Results.Hits)
				{
					var poco = hit.ParseSource<PersonPoco>();
					if (poco == null)
						Console.WriteLine("No Source Result Available");
					else
						Console.WriteLine($"First Result {poco.FirstName} {poco.LastName}, {poco.Designation} ({poco.Age}yo {poco.Gender}) Started {poco.DateOfJoining:d}");
				}
				Console.WriteLine("Ready for Deep Search - This could take a while.");
				Console.WriteLine("Press any key to begin.");
				Console.ReadKey();
				Console.WriteLine("Searching...");
				var deepSearchResult = await mlesClient.DeepSearch();
				Console.WriteLine("ML ES Client Deep Search Complete");
				foreach(var hit in deepSearchResult)
				{
					var poco = hit.ParseSource<PersonPoco>();
					if (poco == null)
						Console.WriteLine("No Source Result Available");
					else
						Console.WriteLine($"First Result {poco.FirstName} {poco.LastName}, {poco.Designation} ({poco.Age}yo {poco.Gender}) Started {poco.DateOfJoining:d}");
				}
				Console.WriteLine($"{deepSearchResult.Count} Deep Search Results");
			}
			catch (Exception ex)
			{
				Console.WriteLine("FATAL ERROR: " + ex.Message);
			}
			Console.WriteLine("Press any key to exit.");
			Console.ReadKey();
		}
	}
	public class PersonPoco
	{
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public string Designation { get; set; }
		public decimal? Salary { get; set; }
		public DateTime? DateOfJoining { get; set; }
		public string Address { get; set; }
		public string Gender { get; set; }
		public int? Age { get; set; }
		public string MaritalStatus { get; set; }
		public string Interests { get; set; }
	}
}
