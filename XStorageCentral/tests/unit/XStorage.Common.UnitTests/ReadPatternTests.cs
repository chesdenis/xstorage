using XStorage.Common.Caching;
using XStorage.Common.ReadPatterns;

namespace XStorage.Common.UnitTests;

public sealed class ReadPatternTests
{
    [Fact]
    public void Sync_WhenFirstLayerHits_ReturnsValue_DoesNotCallSource_DoesNotPromote()
    {
        var log = new List<string>();

        var l0 = new SyncLayer<int>("L0",
            tryRead: () => CacheResult<int>.HitResult(123),
            set: v => log.Add($"L0.Set({v})"));

        var pattern = new SyncReadPattern<int>(l0);

        var sourceCalled = 0;
        int Source()
        {
            sourceCalled++;
            return 999;
        }

        var result = pattern.Execute(Source);

        Assert.Equal(123, result);
        Assert.Equal(0, sourceCalled);
        Assert.Empty(log); // no promotion because hit at i=0 -> PromoteBack(-1) does nothing
    }

    [Fact]
    public void Sync_WhenSecondLayerHits_PromotesToFirstLayer_DoesNotCallSource()
    {
        var log = new List<string>();

        var l0 = new SyncLayer<int>("L0",
            tryRead: () => CacheResult<int>.MissResult(),
            set: v => log.Add($"L0.Set({v})"));

        var l1 = new SyncLayer<int>("L1",
            tryRead: () => CacheResult<int>.HitResult(42),
            set: v => log.Add($"L1.Set({v})"));

        var pattern = new SyncReadPattern<int>(l0).Then(l1);

        var sourceCalled = 0;
        int Source()
        {
            sourceCalled++;
            return 999;
        }

        var result = pattern.Execute(Source);

        Assert.Equal(42, result);
        Assert.Equal(0, sourceCalled);
        Assert.Equal(new[] { "L0.Set(42)" }, log); // promote to earlier layers only
    }

    [Fact]
    public void Sync_WhenAllLayersMiss_CallsSource_AndPromotesToAllLayers_InReverse()
    {
        var log = new List<string>();

        var l0 = new SyncLayer<int>("L0",
            tryRead: () => CacheResult<int>.MissResult(),
            set: v => log.Add($"L0.Set({v})"));

        var l1 = new SyncLayer<int>("L1",
            tryRead: () => CacheResult<int>.MissResult(),
            set: v => log.Add($"L1.Set({v})"));

        var l2 = new SyncLayer<int>("L2",
            tryRead: () => CacheResult<int>.MissResult(),
            set: v => log.Add($"L2.Set({v})"));

        var pattern = new SyncReadPattern<int>(l0).Then(l1).Then(l2);

        var sourceCalled = 0;
        int Source()
        {
            sourceCalled++;
            return 777;
        }

        var result = pattern.Execute(Source);

        Assert.Equal(777, result);
        Assert.Equal(1, sourceCalled);

        // PromoteBack(_layers.Count - 1) means i = 2..0
        Assert.Equal(new[] { "L2.Set(777)", "L1.Set(777)", "L0.Set(777)" }, log);
    }

    [Fact]
    public void Sync_WhenLayerFaults_IsSkipped_AndNextHitIsUsed_AndPromoted()
    {
        var log = new List<string>();

        var l0 = new SyncLayer<int>("L0",
            tryRead: () => CacheResult<int>.FaultResult(new InvalidOperationException("boom")),
            set: v => log.Add($"L0.Set({v})"));

        var l1 = new SyncLayer<int>("L1",
            tryRead: () => CacheResult<int>.HitResult(10),
            set: v => log.Add($"L1.Set({v})"));

        var pattern = new SyncReadPattern<int>(l0).Then(l1);

        var sourceCalled = 0;
        int Source()
        {
            sourceCalled++;
            return 999;
        }

        var result = pattern.Execute(Source);

        Assert.Equal(10, result);
        Assert.Equal(0, sourceCalled);

        // Hit at layer 1 -> promote to layer 0
        Assert.Equal(new[] { "L0.Set(10)" }, log);
    }

    [Fact]
    public void Sync_WhenPromotionThrows_IsIgnored_AndStillReturnsValue()
    {
        var l0 = new SyncLayer<int>("L0",
            tryRead: () => CacheResult<int>.MissResult(),
            set: _ => throw new Exception("set failed"));

        var l1 = new SyncLayer<int>("L1",
            tryRead: () => CacheResult<int>.HitResult(5),
            set: _ => { });

        var pattern = new SyncReadPattern<int>(l0).Then(l1);

        var result = pattern.Execute(() => 999);

        Assert.Equal(5, result); // promotion failure should not break read
    }
    
    [Fact]
    public async Task Async_WhenFirstLayerHits_ReturnsValue_DoesNotCallSource_DoesNotPromote()
    {
        var log = new List<string>();

        var l0 = new AsyncLayer<int>("L0",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.HitResult(123)),
            setAsync: (v, ct) =>
            {
                log.Add($"L0.Set({v})");
                return ValueTask.CompletedTask;
            });

        var pattern = new AsyncReadPattern<int>(l0);

        var sourceCalled = 0;
        Task<int> Source()
        {
            sourceCalled++;
            return Task.FromResult(999);
        }

        var result = await pattern.ExecuteAsync(Source, CancellationToken.None);

        Assert.Equal(123, result);
        Assert.Equal(0, sourceCalled);
        Assert.Empty(log);
    }

    [Fact]
    public async Task Async_WhenSecondLayerHits_PromotesToFirstLayer_DoesNotCallSource()
    {
        var log = new List<string>();

        var l0 = new AsyncLayer<int>("L0",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.MissResult()),
            setAsync: (v, ct) =>
            {
                log.Add($"L0.Set({v})");
                return ValueTask.CompletedTask;
            });

        var l1 = new AsyncLayer<int>("L1",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.HitResult(42)),
            setAsync: (v, ct) =>
            {
                log.Add($"L1.Set({v})");
                return ValueTask.CompletedTask;
            });

        var pattern = new AsyncReadPattern<int>(l0).Then(l1);

        var sourceCalled = 0;
        Task<int> Source()
        {
            sourceCalled++;
            return Task.FromResult(999);
        }

        var result = await pattern.ExecuteAsync(Source, CancellationToken.None);

        Assert.Equal(42, result);
        Assert.Equal(0, sourceCalled);
        Assert.Equal(new[] { "L0.Set(42)" }, log);
    }

    [Fact]
    public async Task Async_WhenAllLayersMiss_CallsSource_AndPromotesToAllLayers_InReverse()
    {
        var log = new List<string>();

        var l0 = new AsyncLayer<int>("L0",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.MissResult()),
            setAsync: (v, ct) => { log.Add($"L0.Set({v})"); return ValueTask.CompletedTask; });

        var l1 = new AsyncLayer<int>("L1",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.MissResult()),
            setAsync: (v, ct) => { log.Add($"L1.Set({v})"); return ValueTask.CompletedTask; });

        var l2 = new AsyncLayer<int>("L2",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.MissResult()),
            setAsync: (v, ct) => { log.Add($"L2.Set({v})"); return ValueTask.CompletedTask; });

        var pattern = new AsyncReadPattern<int>(l0).Then(l1).Then(l2);

        var sourceCalled = 0;
        Task<int> Source()
        {
            sourceCalled++;
            return Task.FromResult(777);
        }

        var result = await pattern.ExecuteAsync(Source, CancellationToken.None);

        Assert.Equal(777, result);
        Assert.Equal(1, sourceCalled);
        Assert.Equal(new[] { "L2.Set(777)", "L1.Set(777)", "L0.Set(777)" }, log);
    }

    [Fact]
    public async Task Async_WhenLayerFaults_IsSkipped_AndNextHitIsUsed_AndPromoted()
    {
        var log = new List<string>();

        var l0 = new AsyncLayer<int>("L0",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.FaultResult(new Exception("redis down"))),
            setAsync: (v, ct) => { log.Add($"L0.Set({v})"); return ValueTask.CompletedTask; });

        var l1 = new AsyncLayer<int>("L1",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.HitResult(10)),
            setAsync: (v, ct) => { log.Add($"L1.Set({v})"); return ValueTask.CompletedTask; });

        var pattern = new AsyncReadPattern<int>(l0).Then(l1);

        var sourceCalled = 0;
        Task<int> Source()
        {
            sourceCalled++;
            return Task.FromResult(999);
        }

        var result = await pattern.ExecuteAsync(Source, CancellationToken.None);

        Assert.Equal(10, result);
        Assert.Equal(0, sourceCalled);
        Assert.Equal(new[] { "L0.Set(10)" }, log);
    }

    [Fact]
    public async Task Async_WhenPromotionThrows_IsIgnored_AndStillReturnsValue()
    {
        var l0 = new AsyncLayer<int>("L0",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.MissResult()),
            setAsync: (v, ct) => ValueTask.FromException(new Exception("set failed")));

        var l1 = new AsyncLayer<int>("L1",
            tryReadAsync: ct => new ValueTask<CacheResult<int>>(CacheResult<int>.HitResult(5)),
            setAsync: (v, ct) => ValueTask.CompletedTask);

        var pattern = new AsyncReadPattern<int>(l0).Then(l1);

        var result = await pattern.ExecuteAsync(() => Task.FromResult(999), CancellationToken.None);

        Assert.Equal(5, result);
    }

    [Fact]
    public async Task Async_WhenTryReadAsyncThrows_ExceptionBubblesOut()
    {
        var l0 = new AsyncLayer<int>("L0",
            tryReadAsync: ct => throw new InvalidOperationException("layer threw"),
            setAsync: (v, ct) => ValueTask.CompletedTask);

        var pattern = new AsyncReadPattern<int>(l0);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            pattern.ExecuteAsync(() => Task.FromResult(1), CancellationToken.None));
    }
}
