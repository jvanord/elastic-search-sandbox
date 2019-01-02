using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace ElasticSearchSandbox
{
	public class ElasticSearchClient
	{
		private static readonly HttpClient _httpClient = new HttpClient();
		private readonly Uri _queryUri;
		private readonly string _queryBody;
		public ElasticSearchClient(string queryUri, string queryBody = null)
		{
			_queryUri = new Uri(queryUri);
			_queryBody = queryBody;
		}
		public async void Ping()
		{
			var request = (HttpWebRequest)HttpWebRequest.Create(_queryUri);
			request.Timeout = 3000;
			request.Method = "HEAD";
			using (var response = await request.GetResponseAsync())
			{
				/* No Exception = Success */
			}
		}
		public async Task<ElasticSearchResponse> Search() => await Search(false);
		public async Task<ElasticSearchResponse> SearchWithScroll() => await Search(true);
		private async Task<ElasticSearchResponse> Search(bool scroll)
		{
			var request = new HttpRequestMessage
			{
				RequestUri = _queryUri,
				Method = HttpMethod.Get
			};
			if (!string.IsNullOrWhiteSpace(_queryBody))
			{
				request.Content = new StringContent(_queryBody);
				request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
			}
			var result = await _httpClient.SendAsync(request).ConfigureAwait(false);
			result.EnsureSuccessStatusCode();
			var responseBody = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
			return ElasticSearchResponse.Parse(responseBody);
		}
	}
	public class ElasticSearchResponse
	{
		private ElasticSearchResponse() { }

		[JsonProperty("_scroll_id")]
		public string ScrollId { get; private set; }

		[JsonProperty("timed_out")]
		public bool TimedOut { get; set; }

		[JsonProperty("hits")]
		public ElasticSearchHitsResult Results { get; private set; }

		public static ElasticSearchResponse Parse(string jsonResponseBody)
		{
			if (string.IsNullOrWhiteSpace(jsonResponseBody)) 
				throw new ElasticSearchClientException("Cannot Evaluate Empty Response");
			return JsonConvert.DeserializeObject<ElasticSearchResponse>(jsonResponseBody);
		}
	}

	public class ElasticSearchHitsResult
	{
		[JsonProperty("total")]
		public int Total { get; set; }

		[JsonProperty("hits")]
		public List<ElasticSearchHit> Hits { get; set; }
	}

	public class ElasticSearchHit
	{
		[JsonProperty("_id")]
		public string Id { get; set; }

		[JsonProperty("_index")]
		public string Index { get; set; }

		[JsonProperty("_type")]
		public string Type { get; set; }

		[JsonProperty("_score")]
		public decimal Score { get; set; }

		[JsonProperty("_source")]
		public object RawSource { get; set; }

		public T ParseSource<T>()
		{
			if (string.IsNullOrWhiteSpace(RawSource.ToString())) return default(T);
			try
			{
				return JsonConvert.DeserializeObject<T>(RawSource.ToString());
			}
			catch
			{
				return default(T);
			}
		}
	}

	public class ElasticSearchClientException : Exception
	{
		public ElasticSearchClientException(string message) : base(message) { }
	}
}
