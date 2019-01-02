using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace ElasticSearchSandbox
{
	public class ElasticSearchClient
	{
		private static readonly HttpClient _httpClient = new HttpClient();
		private readonly ElasticSearchEndpoint _queryEndpoint;
		private readonly string _queryBody;
		public ElasticSearchClient(string queryUri, string queryBody = null)
		{
			_queryEndpoint = new ElasticSearchEndpoint(queryUri);
			_queryBody = queryBody;
		}
		public async void Ping()
		{
			var request = (HttpWebRequest)HttpWebRequest.Create(_queryEndpoint.Base);
			request.Timeout = 3000;
			request.Method = "HEAD";
			using (var response = await request.GetResponseAsync())
			{
				/* No Exception = Success */
			}
		}

		#region Search

		public async Task<ElasticSearchResponse> Search() => await Search(false);
		public async Task<ElasticSearchResponse> SearchWithScroll() => await Search(true);

		private async Task<ElasticSearchResponse> Search(bool scroll)
		{
			var request = new HttpRequestMessage
			{
				RequestUri = new Uri(_queryEndpoint.Search(scroll)),
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

		#endregion

		#region Scroll

		public async Task<ElasticSearchResponse> Scroll(ElasticSearchResponse searchResponse)
		{
			var scrollId = searchResponse?.ScrollId;
			if (string.IsNullOrWhiteSpace(scrollId))
				throw new ElasticSearchClientException("Scroll ID Required");
			var url = _queryEndpoint.Scroll();
			var body = JsonConvert.SerializeObject(new
			{
				scroll = "1m",
				scroll_id = scrollId
			});
			var content = new StringContent(body, Encoding.UTF8, "application/json");
			var result = await _httpClient.PostAsync(url, content).ConfigureAwait(false);
			result.EnsureSuccessStatusCode();
			var responseBody = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
			return ElasticSearchResponse.Parse(responseBody);
		}

		#endregion

		#region Deep Search

		/// <summary>WARNING: This will perform mutliple requests to the search index and may consume an enormous amount of resources.</summary>
		public async Task<List<ElasticSearchHit>> DeepSearch()
		{
			var allResults = new List<ElasticSearchHit>();
			var thisResult = await Search();
			allResults.AddRange(thisResult.Results.Hits);
			while (thisResult.Results.Hits.Any())
			{
				thisResult = await Scroll(thisResult);
				allResults.AddRange(thisResult.Results.Hits);
			}
			return allResults;
		}

		#endregion
	}

	public class ElasticSearchEndpoint
	{
		private static readonly string _scrollValue = "1m";
		public ElasticSearchEndpoint(string uriString)
		{
			var uri = new Uri(uriString);
			Base = uri.GetLeftPart(UriPartial.Path).Replace("/_search", string.Empty, StringComparison.CurrentCultureIgnoreCase);
			Query = uri.Query;
		}
		public string Base { get; private set; }
		public string Query { get; private set; }
		public string Search(bool scroll)
		{
			var uriBuilder = new StringBuilder(Base);
			if (!Base.EndsWith("/")) uriBuilder.Append("/");
			if (scroll)
			{
				if (string.IsNullOrWhiteSpace(Query))
					Query = "scroll=" + _scrollValue;
				else
					Query += "&scroll=" + _scrollValue;
			}
			return Base + "/_search?" + Query;
		}
		public string Scroll()
		{
			// assumes the last part of the Base URL is the index name, so removes it
			var scrollBase = Base.TrimEnd('/');
			scrollBase = scrollBase.Remove(scrollBase.LastIndexOf('/') + 1);
			return scrollBase + "_search/scroll";
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
