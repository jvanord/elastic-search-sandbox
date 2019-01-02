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
				var searchResponse = await mlesClient.Search();
				Console.WriteLine("ML ES Client Search Complete");
				Console.WriteLine($"{searchResponse.Results.Total} Total Hits");
				Console.WriteLine($"{searchResponse.Results.Hits.Count} Hits This Page");
				//var firstMatch = searchResponse.Results.Hits.FirstOrDefault();
				foreach(var hit in searchResponse.Results.Hits)
				{
					var firstPoco = hit.ParseSource<PersonPoco>();
					if (firstPoco == null)
						Console.WriteLine("No Source Result Available");
					else
						Console.WriteLine($"First Result {firstPoco.FirstName} {firstPoco.LastName}, {firstPoco.Designation} ({firstPoco.Age}yo {firstPoco.Gender}) Started {firstPoco.DateOfJoining}");
				}
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
