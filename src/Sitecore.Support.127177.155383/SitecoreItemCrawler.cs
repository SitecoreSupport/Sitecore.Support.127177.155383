// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SitecoreItemCrawler.cs" company="Sitecore">
//   Copyright (c) Sitecore. All rights reserved.
// </copyright>
// <summary>
//   The sitecore item crawler.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

using Sitecore.Data.LanguageFallback;

namespace Sitecore.Support.ContentSearch
{
  using Data;
  using Data.Managers;
  using SecurityModel;
  using Sitecore.Abstractions;

  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.Data.Items;
  using Sitecore.Diagnostics;
  using System;
  using System.Linq;
  using System.Threading;

  /// <summary>
  /// The sitecore item crawler.
  /// </summary>
  public class SitecoreItemCrawler : Sitecore.ContentSearch.SitecoreItemCrawler
  {
    /// <summary>
    /// Executes the update event.
    /// </summary>
    /// <param name="context">The context.</param>
    /// <param name="indexable">The indexable.</param>
    /// <param name="operationContext">The operation context.</param>
    protected override void DoUpdate(IProviderUpdateContext context, SitecoreIndexableItem indexable, IndexEntryOperationContext operationContext)
    {
      Assert.ArgumentNotNull(context, "context");
      Assert.ArgumentNotNull(indexable, "indexable");

      using (new LanguageFallbackItemSwitcher(this.Index.EnableItemLanguageFallback))
      {
        if (this.IndexUpdateNeedDelete(indexable))
        {
          this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:deleteitem", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
          this.Operations.Delete(indexable, context);
          return;
        }

        this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updatingitem", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
        if (!this.IsExcludedFromIndex(indexable, true))
        {
          /*************************************************************/
          if (operationContext != null && !operationContext.NeedUpdateAllVersions)
          {
            this.UpdateItemVersion(context, indexable, operationContext);
          }
          else
          {
            var languages = (operationContext != null && !operationContext.NeedUpdateAllLanguages) ? new[] { indexable.Item.Language } : indexable.Item.Languages;


            foreach (var language in languages)
            {
              Item latestVersion;
              // Sitecore.Support.155383 patch. Switched from SitecoreCachesDisabler() to WriteCachesDisabler()
              using (new WriteCachesDisabler())
              {
                latestVersion = indexable.Item.Database.GetItem(indexable.Item.ID, language, Data.Version.Latest);
              }

              if (latestVersion == null)
              {
                CrawlingLog.Log.Warn(string.Format("SitecoreItemCrawler : Update : Latest version not found for item {0}. Skipping.", indexable.Item.Uri));
                continue;
              }

              Item[] versions;
              using (new WriteCachesDisabler())
              {
                versions = latestVersion.Versions.GetVersions(false);
              }

              foreach (var version in versions)
              {
                this.UpdateItemVersion(context, version, operationContext);
              }
            }
          }

          /*************************************************************/
          this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updateditem", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
        }

        if (this.DocumentOptions.ProcessDependencies)
        {
          this.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:updatedependents", this.index.Name, indexable.UniqueId, indexable.AbsolutePath);
          this.UpdateDependents(context, indexable);
        }
      }
    }

    // Sitecore.Support.127177 patch. LanguageFallback context was added to take the index language fallback configuration into account.
    protected override SitecoreIndexableItem GetIndexableAndCheckDeletes(IIndexableUniqueId indexableUniqueId)
    {
      using (new LanguageFallbackItemSwitcher(new bool?(base.index.EnableItemLanguageFallback)))
      {
        ItemUri itemUri = indexableUniqueId as SitecoreItemUniqueId;

        using (new SecurityDisabler())
        {
          Item item;
          using (new WriteCachesDisabler())
          {
            item = Data.Database.GetItem(itemUri);
          }

          if (item != null)
          {
            var latestItemUri = new ItemUri(itemUri.ItemID, itemUri.Language, Data.Version.Latest, itemUri.DatabaseName);
            var latestItem = Data.Database.GetItem(latestItemUri);

            Data.Version[] versions;
            using (new WriteCachesDisabler())
            {
              versions = latestItem.Versions.GetVersionNumbers() ?? new Data.Version[0];
            }

            if (itemUri.Version != Data.Version.Latest && versions.All(v => v.Number != itemUri.Version.Number))
              item = null;
          }

          return item;
        }

      }
    }

    /// <summary>
    /// Updates specific item.
    /// </summary>
    /// <param name="context">The context.</param>
    /// <param name="indexableUniqueId">The indexable unique id.</param>
    /// <param name="operationContext">The operation context.</param>
    /// <param name="indexingOptions">The indexing options.</param>
    public override void Update(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexEntryOperationContext operationContext, IndexingOptions indexingOptions = IndexingOptions.Default)
    {
      Assert.ArgumentNotNull(indexableUniqueId, "indexableUniqueId");

      var contextEx = context as ITrackingIndexingContext;
      var skipIndexable = contextEx != null && !contextEx.Processed.TryAdd(indexableUniqueId, null);

      if (skipIndexable || !ShouldStartIndexing(indexingOptions))
        return;

      var options = this.DocumentOptions;
      Assert.IsNotNull(options, "DocumentOptions");

      if (this.IsExcludedFromIndex(indexableUniqueId, operationContext, true))
        return;

      if (operationContext != null)
      {
        if (operationContext.NeedUpdateChildren)
        {
          var item = Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);

          if (item != null)
          {
            // check if we moved item out of the index's root.
            bool needDelete = operationContext.OldParentId != Guid.Empty
                   && this.IsRootOrDescendant(new ID(operationContext.OldParentId))
                   && !this.IsAncestorOf(item);

            if (needDelete)
            {
              this.Delete(context, indexableUniqueId);
              return;
            }

            this.UpdateHierarchicalRecursive(context, item, CancellationToken.None);
            return;
          }
        }

        if (operationContext.NeedUpdatePreviousVersion)
        {
          var item = Data.Database.GetItem(indexableUniqueId as SitecoreItemUniqueId);
          if (item != null)
          {
            this.UpdatePreviousVersion(item, context);
          }
        }
      }

      var indexable = this.GetIndexableAndCheckDeletes(indexableUniqueId);

      if (indexable == null)
      {
        if (this.GroupShouldBeDeleted(indexableUniqueId.GroupId))
        {
          this.Delete(context, indexableUniqueId.GroupId);
          return;
        }

        this.Delete(context, indexableUniqueId);
        return;
      }

      this.DoUpdate(context, indexable, operationContext);
    }

    private bool IsRootOrDescendant(ID id)
    {
      if (this.RootItem.ID == id)
      {
        return true;
      }

      var factory = ContentSearchManager.Locator.GetInstance<IFactory>();
      Database db = factory.GetDatabase(this.Database);
      Item oldParent;
      using (new SecurityDisabler())
      {
        oldParent = db.GetItem(id);
      }

      if (oldParent != null && this.IsAncestorOf(oldParent))
      {
        return true;
      }

      return false;
    }

    private void UpdatePreviousVersion(Item item, IProviderUpdateContext context)
    {
      Data.Version[] versions;
      using (new WriteCachesDisabler())
      {
        versions = item.Versions.GetVersionNumbers() ?? new Data.Version[0];
      }

      int indexOfItem = versions.ToList().FindIndex(version => version.Number == item.Version.Number);
      if (indexOfItem < 1)
      {
        return;
      }

      var previousVersion = versions[indexOfItem - 1];

      var previousItemVersion = versions.FirstOrDefault(version => version == previousVersion);
      var previousItemUri = new ItemUri(item.ID, item.Language, previousItemVersion, item.Database.Name);
      var previousItem = Data.Database.GetItem(previousItemUri);
      var versionIndexable = (SitecoreIndexableItem)previousItem;

      if (versionIndexable != null)
      {
        var versionBuiltinFields = (IIndexableBuiltinFields)versionIndexable;
        versionBuiltinFields.IsLatestVersion = false;
        versionIndexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;

        this.Operations.Update(versionIndexable, context, this.index.Configuration);
      }
    }

    public virtual void Delete(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions = IndexingOptions.Default)
    {
      if (!ShouldStartIndexing(indexingOptions))
        return;

      context.Index.Locator.GetInstance<IEvent>().RaiseEvent("indexing:deleteitem", this.index.Name, indexableUniqueId);
      this.Operations.Delete(indexableUniqueId, context);

      var deleteDependencies = this.GetIndexablesToUpdateOnDelete(indexableUniqueId);
      var deleteDependencyIndexables = deleteDependencies.Select(this.GetIndexable).Where(i => i != null);

      IndexEntryOperationContext operationContext = new IndexEntryOperationContext
      {
        NeedUpdateAllLanguages = false,
        NeedUpdateAllVersions = false,
        NeedUpdateChildren = false
      };

      // Sitecore.Support.127177 patch. Takes into account LanguageFallback configuration of the index.
      if (index.EnableItemLanguageFallback)
      {
        this.DoUpdateFallbackField(context, indexableUniqueId);
        foreach (var deleteDependencyIndexable in deleteDependencyIndexables)
        {
          this.DoUpdate(context, deleteDependencyIndexable, operationContext);
        }
      }
    }

    private void DoUpdateFallbackField(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId)
    {
      var indexableItem = this.GetIndexable(indexableUniqueId) as SitecoreIndexableItem;
      if (indexableItem == null)
      {
        return;
      }
      indexableItem.Item.Fields.ReadAll();
      var fields = indexableItem.Item.Fields;
      if (fields.Any(e => e.SharedLanguageFallbackEnabled))
      {
        var fallbackItems = LanguageFallbackManager.GetDependentLanguages(indexableItem.Item.Language, indexableItem.Item.Database, indexableItem.Item.ID)
          .SelectMany(
            language =>
            {
              var item1 = indexableItem.Item.Database.GetItem(indexableItem.Item.ID, language);
              return item1 != null ? item1.Versions.GetVersions() : new Item[0];
            });

        foreach (var fallbackItem in fallbackItems)
        {
          var valueToConsider = new SitecoreItemUniqueId(fallbackItem.Uri);
          this.Update(context, valueToConsider);
        }
      }
    }
  }
}
