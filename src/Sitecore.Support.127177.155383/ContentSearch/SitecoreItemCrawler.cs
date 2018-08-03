using Sitecore.Data.LanguageFallback;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using System;
using System.Linq;
using System.Threading;
using Sitecore.Data;
using Sitecore.Data.Managers;
using Sitecore.Globalization;
using Sitecore.SecurityModel;

namespace Sitecore.Support.ContentSearch
{
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

        using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
        {
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
                using (new WriteCachesDisabler())
                using (new SecurityDisabler())
                {
                  latestVersion = indexable.Item.Database.GetItem(indexable.Item.ID, language, Sitecore.Data.Version.Latest);
                }

                if (latestVersion == null)
                {
                  CrawlingLog.Log.Warn(string.Format("SitecoreItemCrawler : Update : Latest version not found for item {0}. Skipping.", indexable.Item.Uri));
                  continue;
                }

                Item[] versions;
                using (new SitecoreCachesDisabler())
                {
                  versions = !latestVersion.IsFallback
                      ? latestVersion.Versions.GetVersions(false)
                      : new[] { latestVersion };

                  DeleteUnusedFallbackVersion(latestVersion, context);
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
    }

    /// <summary>
    /// Deletes former fallback version in case fallback is switched off on the current item
    /// </summary>
    /// <param name="version">The item version</param>
    /// <param name="context">update context</param>
    private void DeleteUnusedFallbackVersion(Item version, IProviderUpdateContext context)
    {
      if (LanguageFallbackItemSwitcher.CurrentValue == true &&
          !version.LanguageFallbackEnabled &&
          version.RuntimeSettings.TemporaryVersion)
      {
        Operations.Delete(PrepareIndexableVersion(version, context), context);
      }
    }

    /// <summary>
    /// Prepares the indexable version.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <param name="context">The context.</param>
    /// <returns>indexable item object</returns>
    internal SitecoreIndexableItem PrepareIndexableVersion(Item item, IProviderUpdateContext context)
    {
      var indexable = (SitecoreIndexableItem)item;
      var cloneBuiltinFields = (IIndexableBuiltinFields)indexable;
      cloneBuiltinFields.IsLatestVersion = item.Versions.IsLatestVersion();
      indexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;
      return indexable;
    }

    protected override SitecoreIndexableItem GetIndexableAndCheckDeletes(IIndexableUniqueId indexableUniqueId)
    {
      ItemUri itemUri = indexableUniqueId as SitecoreItemUniqueId;

      using (new SecurityDisabler())
      {
        Item item;
        using (new WriteCachesDisabler())
        {
          item = GetItem(itemUri);
        }

        if (item != null)
        {
          var latestItemUri = new ItemUri(itemUri.ItemID, itemUri.Language, Sitecore.Data.Version.Latest, itemUri.DatabaseName);
          var latestItem = GetItem(latestItemUri);

          Sitecore.Data.Version[] versions;
          using (new WriteCachesDisabler())
          {
            versions = latestItem.Versions.GetVersionNumbers() ?? new Sitecore.Data.Version[0];
          }

          if (itemUri.Version != Sitecore.Data.Version.Latest && versions.All(v => v.Number != itemUri.Version.Number))
            item = null;
        }

        return item;
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

      var skipIndexable = !this.CircularReferencesIndexingGuard.TryAddToProcessedList(indexableUniqueId, this, context);

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
          var item = GetIndexable(indexableUniqueId as SitecoreItemUniqueId);

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
          var item = GetIndexable(indexableUniqueId as SitecoreItemUniqueId);
          if (item != null)
          {
            this.UpdatePreviousVersion(item, context);
          }
        }

        if (operationContext.NeedUpdateAllVersions)
        {
          var item = GetItem(indexableUniqueId as SitecoreItemUniqueId);
          if (item != null)
          {
            this.DoUpdate(context, item, operationContext);
            return;
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

    /// <summary>
    /// Determines whether the id is equal to the root ID or is a descendant of the root.
    /// </summary>
    /// <param name="id">The identifier.</param>
    /// <returns><c>true</c> if the id equals to root's ID or it is its descendant; otherwise, false.</returns>
    private bool IsRootOrDescendant(ID id)
    {
      if (this.RootItem.ID == id)
      {
        return true;
      }

      Item oldParent;
      using (new SecurityDisabler())
      {
        oldParent = GetItem(id);
      }

      if (oldParent != null && this.IsAncestorOf(oldParent))
      {
        return true;
      }

      return false;
    }

    /// <summary>
    /// Updates the previous version.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <param name="context">The context.</param>
    private void UpdatePreviousVersion(Item item, IProviderUpdateContext context)
    {
      Sitecore.Data.Version[] versions;
      using (new WriteCachesDisabler())
      {
        versions = item.Versions.GetVersionNumbers() ?? new Sitecore.Data.Version[0];
      }

      int indexOfItem = versions.ToList().FindIndex(version => version.Number == item.Version.Number);
      if (indexOfItem < 1)
      {
        return;
      }

      var previousVersion = versions[indexOfItem - 1];

      var previousItemVersion = versions.FirstOrDefault(version => version == previousVersion);
      var previousItemUri = new ItemUri(item.ID, item.Language, previousItemVersion, item.Database.Name);
      var previousItem = Sitecore.Data.Database.GetItem(previousItemUri);
      var versionIndexable = (SitecoreIndexableItem)previousItem;

      if (versionIndexable != null)
      {
        var versionBuiltinFields = (IIndexableBuiltinFields)versionIndexable;
        versionBuiltinFields.IsLatestVersion = false;
        versionIndexable.IndexFieldStorageValueFormatter = context.Index.Configuration.IndexFieldStorageValueFormatter;

        this.Operations.Update(versionIndexable, context, this.index.Configuration);
      }
    }

    public override void Delete(IProviderUpdateContext context, IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions = IndexingOptions.Default)
    {
      base.Delete(context, indexableUniqueId, indexingOptions);
      if (context.Index.EnableItemLanguageFallback)
      {
        this.DeleteFallbackItem(indexableUniqueId, context);
      }
    }

    /// <summary>
    /// When perform a version deletion operation,
    /// Make sure that all the fallback language version deleted if it is the last item of main language.
    /// </summary>
    /// <param name="context">The context.</param>
    /// <param name="id">The version indexable.</param>
    private void DeleteFallbackItem(IIndexableUniqueId id, IProviderUpdateContext context)
    {
      if (new ItemUri(id as SitecoreItemUniqueId).DatabaseName != this.Database)
      {
        return;
      }

      var item = Sitecore.Data.Database.GetItem(id as SitecoreItemUniqueId);
      if (item == null)
      {
        return;
      }
      var items = LanguageFallbackManager.GetDependentLanguages(item.Language, item.Database, item.ID)
        .Select(
          language =>
          {
            var item1 = item.Database.GetItem(item.ID, language, item.Version);
            return item1;
          })
        .Where(item1 => item1.RuntimeSettings.TemporaryVersion);

      foreach (var indexableItem in items.Select(sitecoreItem => this.PrepareIndexableVersion(sitecoreItem, context)))
      {
        this.Operations.Delete(indexableItem, context);
      }
    }
  }
}