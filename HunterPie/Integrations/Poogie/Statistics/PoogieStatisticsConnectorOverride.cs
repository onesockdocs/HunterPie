using HunterPie.Core.Domain.Cache;
using HunterPie.Integrations.Poogie.Common;
using HunterPie.Integrations.Poogie.Common.Models;
using HunterPie.Integrations.Poogie.Statistics.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace HunterPie.Integrations.Poogie.Statistics;

internal class PoogieStatisticsConnector
{
    private const string FileName = "hunts.ndjson";

    private readonly IAsyncCache _unusedCache;
    private readonly IPoogieClientAsync _unusedClient;

    private readonly string _filePath;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private List<StoredHunt>? _storedHunts;

    public PoogieStatisticsConnector(IAsyncCache cache, IPoogieClientAsync client)
    {
        _unusedCache = cache;
        _unusedClient = client;

        string folder = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "HunterPie",
            "Statistics");

        Directory.CreateDirectory(folder);
        _filePath = Path.Combine(folder, FileName);
    }

    public async Task<PoogieResult<PoogieQuestStatisticsModel>> UploadAsync(PoogieQuestStatisticsModel model)
    {
        await EnsureLoadedAsync().ConfigureAwait(false);

        StoredHunt? existing =
            _storedHunts!.FirstOrDefault(h => h.Statistics.Hash == model.Hash);

        if (existing == null)
        {
            string id = Guid.NewGuid().ToString();
            var hunt = new StoredHunt(id, model);
            _storedHunts!.Add(hunt);
            await AppendAsync(hunt).ConfigureAwait(false);
        }
        else
        {
            existing.Statistics = model;

            var updated = new StoredHunt(existing.Id, model);
            _storedHunts!.Remove(existing);
            _storedHunts!.Add(updated);

            await AppendAsync(updated).ConfigureAwait(false);
        }

        return new PoogieResult<PoogieQuestStatisticsModel>(model, null);
    }

    public async Task<PoogieResult<Paginated<PoogieQuestSummaryModel>>> GetUserQuestSummariesV2(int page, int limit)
    {
        await EnsureLoadedAsync().ConfigureAwait(false);

        if (page < 1)
            page = 1;

        if (limit < 1)
            limit = 1;

        var allSummaries = _storedHunts!
            .Select(ToSummary)
            .OrderByDescending(s => s.CreatedAt)
            .ToList();

        int total = allSummaries.Count;
        int skip = (page - 1) * limit;

        if (skip > total)
            skip = Math.Max(0, total - limit);

        if (skip < 0)
            skip = 0;

        PoogieQuestSummaryModel[] pageElements = allSummaries
            .Skip(skip)
            .Take(limit)
            .ToArray();

        int totalPages = total == 0 ? 1 : (int)Math.Ceiling(total / (double)limit);

        var paginated = new Paginated<PoogieQuestSummaryModel>(totalPages, page, pageElements);

        return new PoogieResult<Paginated<PoogieQuestSummaryModel>>(paginated, null);
    }

    public async Task<PoogieResult<List<PoogieQuestSummaryModel>>> GetUserQuestSummariesAsync()
    {
        await EnsureLoadedAsync().ConfigureAwait(false);

        var summaries = _storedHunts!
            .Select(ToSummary)
            .OrderByDescending(s => s.CreatedAt)
            .ToList();

        return new PoogieResult<List<PoogieQuestSummaryModel>>(summaries, null);
    }

    public async Task<PoogieResult<PoogieQuestStatisticsModel>> GetAsync(string uploadId)
    {
        await EnsureLoadedAsync().ConfigureAwait(false);

        StoredHunt? hunt = _storedHunts!
            .FirstOrDefault(h => h.Id == uploadId);

        PoogieQuestStatisticsModel? stats = hunt?.Statistics;

        return new PoogieResult<PoogieQuestStatisticsModel>(stats, null);
    }

    private async Task EnsureLoadedAsync()
    {
        if (_storedHunts != null)
            return;

        await _lock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (_storedHunts != null)
                return;

            var huntsByHash = new Dictionary<string, StoredHunt>();

            if (File.Exists(_filePath))
            {
                using var stream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var reader = new StreamReader(stream);

                while (true)
                {
                    string? line = await reader.ReadLineAsync().ConfigureAwait(false);
                    if (line == null)
                        break;

                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    StoredHunt? hunt;
                    try
                    {
                        hunt = JsonConvert.DeserializeObject<StoredHunt>(line);
                    }
                    catch
                    {
                        continue;
                    }

                    if (hunt?.Statistics?.Hash == null)
                        continue;

                    huntsByHash[hunt.Statistics.Hash] = hunt;
                }
            }

            _storedHunts = huntsByHash.Values.ToList();
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task AppendAsync(StoredHunt hunt)
    {
        await _lock.WaitAsync().ConfigureAwait(false);

        try
        {
            string line = JsonConvert.SerializeObject(hunt, Formatting.None);

            using var stream = new FileStream(_filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            using var writer = new StreamWriter(stream);

            await writer.WriteLineAsync(line).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }
    }

    private static PoogieQuestSummaryModel ToSummary(StoredHunt stored)
    {
        PoogieQuestStatisticsModel s = stored.Statistics;
        TimeSpan? elapsed = s.FinishedAt > s.StartedAt
            ? s.FinishedAt - s.StartedAt
            : (TimeSpan?)null;

        PoogieMonsterSummaryModel[] monsters =
            Array.Empty<PoogieMonsterSummaryModel>();

        return new PoogieQuestSummaryModel(
            Id: stored.Id,
            GameType: s.GameType,
            QuestDetails: s.Quest,
            ElapsedTime: elapsed,
            Monsters: monsters,
            CreatedAt: s.UploadedAt
        );
    }

    private class StoredHunt
    {
        public string Id { get; set; }
        public PoogieQuestStatisticsModel Statistics { get; set; }

        public StoredHunt(string id, PoogieQuestStatisticsModel statistics)
        {
            Id = id;
            Statistics = statistics;
        }
    }
}