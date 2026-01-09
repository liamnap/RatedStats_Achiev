local RatedStats, Achiev = ...
local playerName   = UnitName("player") .. "-" .. GetRealmName()
local lastMatchActive = 0

local f = CreateFrame("Frame")
f:RegisterEvent("ADDON_LOADED")
f:SetScript("OnEvent", function(self, event, addonName)
    if addonName ~= "RatedStats" then return end

    -- now it's safe to grab the namespace
    local RSTATS = _G.RSTATS

    if RSTATS then
        RSTATS:RegisterModule("Achiev", "Achievement Tooltip Enhancements", true)
    end

    -- we don't need this frame any more
    self:UnregisterEvent("ADDON_LOADED")
end)

-- Determine the region and use the appropriate achievement data
local regionMap = {
    [1] = "US",
    [2] = "KR",
    [3] = "EU",
    [4] = "TW",
}

local regionID = GetCurrentRegion()
local regionCode = regionMap[regionID] or "US"

-- Normalize realm display names (Twilight's Hammer, Pozzo dell'Eternità, etc.)
-- to the slug format used by the Blizzard API / our region files:
--  - lowercase
--  - spaces -> '-'
--  - apostrophes removed
--  - common accents stripped
local function NormalizeRealmSlug(realm)
    if not realm or realm == "" then
        realm = GetRealmName() or ""
    end

    -- Fix cases like "Twilight'sHammer" → "Twilight's Hammer"
    -- i.e. "'s" immediately followed by an uppercase letter.
    -- Do this *before* lowercasing so we can detect %u properly.
    realm = realm:gsub("(%l's)(%u)", "%1 %2")

    realm = realm:lower()

    -- remove apostrophes and similar
    realm = realm:gsub("['’`]", "")

    -- spaces become hyphens
    realm = realm:gsub("%s+", "-")

    -- strip common Latin-1 accents
    realm = realm
        :gsub("[àáâä]", "a")
        :gsub("[èéêë]", "e")
        :gsub("[ìíîï]", "i")
        :gsub("[òóôö]", "o")
        :gsub("[ùúûü]", "u")

    return realm
end

-- Merge monolithic or chunked achievement files into one table
local function mergeRegionParts(region)
    local merged = {}
    local baseName = "ACHIEVEMENTS_" .. region
    local base = _G[baseName]
    if type(base) == "table" then
        for _, v in ipairs(base) do
            table.insert(merged, v)
        end
    end

    local partIndex = 1
    while true do
        local part = _G[baseName .. "_PART" .. partIndex]
        if type(part) ~= "table" then break end
        for _, v in ipairs(part) do
            table.insert(merged, v)
        end
        partIndex = partIndex + 1
    end

    return merged
end

local regionData = mergeRegionParts(regionCode)

-- Cache table to avoid repeat lookups
local achievementCache = {}

-- 🔹 Build fast lookup index for character → entry
local regionLookup = {}
for _, entry in ipairs(regionData) do
    -- main character key
    if entry.character then
        regionLookup[entry.character:lower()] = entry
    end
    -- alt keys: point each alt at the same entry as the main
    if entry.alts and type(entry.alts) == "table" then
        for _, altName in ipairs(entry.alts) do
            if type(altName) == "string" and altName ~= "" then
                regionLookup[altName:lower()] = entry
            end
        end
    end
end

local R1Titles = {
    "Primal Gladiator", "Wild Gladiator", "Warmongering Gladiator",
    "Vindictive Gladiator", "Fearless Gladiator", "Cruel Gladiator",
    "Ferocious Gladiator", "Fierce Gladiator", "Demonic Gladiator",
    "Dread Gladiator", "Sinister Gladiator", "Notorious Gladiator",
    "Corrupted Gladiator", "Sinful Gladiator", "Unchained Gladiator",
    "Cosmic Gladiator", "Eternal Gladiator", "Crimson Gladiator",
    "Obsidian Gladiator", "Draconic Gladiator", "Seasoned Gladiator",
    "Forged Warlord", "Forged Marshal", "Forged Legend", "Forged Gladiator",
    "Prized Warlord", "Prized Marshal", "Prized Legend", "Prized Gladiator",
    "Astral Warlord", "Astral Marshal", "Astral Legend", "Astral Gladiator"
}

local HeroTitles = {
    "Hero of the Horde",
    "Hero of the Alliance"
}

local PvpRankColumns = {
    { key = "a",  label = "Combat (1400)",         prefix = "Combatant I:",   icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_02_Small.blp" },
    { key = "b",  label = "Combat (1500)",         prefix = "Combatant II:",  icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_02_Small.blp" },
    { key = "c",  label = "Chall (1600)",          prefix = "Challenger I:",  icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_03_Small.blp" },
    { key = "d",  label = "Chall (1700)",          prefix = "Challenger II:", icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_03_Small.blp" },
    { key = "e",  label = "Rival (1800)",          prefix = "Rival I:",       icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_04_Small.blp" },
    { key = "f",  label = "Rival (1950)",          prefix = "Rival II:",      icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_04_Small.blp" },
    { key = "g",  label = "Duelist (2100)",        prefix = "Duelist:",       icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_05_Small.blp" },
    { key = "h1", label = "General (2200)",        prefix = "General",        icon = "Interface\\PvPRankBadges\\PvPRank12.blp", hidden = true },
    { key = "h2", label = "Marshal (2200)",        prefix = "Marshal",        icon = "Interface\\PvPRankBadges\\PvPRank12.blp", hidden = true },
    { key = "h3", label = "Warlord (2300)",        prefix = "Warlord",        icon = "Interface\\PvPRankBadges\\PvPRank13.blp", hidden = true },
    { key = "h4", label = "Field Marshal (2300)",  prefix = "Field Marshal",  icon = "Interface\\PvPRankBadges\\PvPRank13.blp", hidden = true },
    { key = "h5", label = "High Warlord (2400)",   prefix = "High Warlord",   icon = "Interface\\PvPRankBadges\\PvPRank14.blp", hidden = true },
    { key = "h6", label = "Grand Marshal (2400)",  prefix = "Grand Marshal",  icon = "Interface\\PvPRankBadges\\PvPRank14.blp", hidden = true },
    { key = "i",  label = "Elite (2400)",          prefix = "Elite:",         icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_06_Small.blp" },
    { key = "j",  label = "Strategist (2400)",     prefix = "Strategist:",    icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_07_Small.blp", tint = { 0.20, 1.00, 0.20 } }, -- brighter green
    { key = "k",  label = "Glad (2400)",           prefix = "Gladiator:",     icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_07_Small.blp", tint = { 1.00, 0.35, 0.95 } }, -- brighter pink
    { key = "l",  label = "Legend (2400)",         prefix = "Legend:",        icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_07_Small.blp", tint = { 1.00, 0.35, 0.20 } }, -- brighter red
    { key = "m1", label = "Three's Company (2700)",prefix = "Three's Company",icon = "Interface\\Icons\\Achievement_Arena_3v3_7", hidden = true },
    { key = "n",  label = "Rank 1 (0.1%)",         r1 = true,                 icon = "Interface\\PVPFrame\\Icons\\UI_RankedPvP_07_Small.blp" },
    { key = "o",  label = "Hero (0.5%)",           hero = true,               icons = {
        "Interface\\PvPRankBadges\\PvPRankHorde.blp",
        "Interface\\PvPRankBadges\\PvPRankAlliance.blp"
    }},
}

-- Center text in fixed-width column
local function centerText(text, width)
    local str = tostring(text)
    local len = #str
    local pad = math.max(0, math.floor((width - len) / 2))
    return string.rep(" ", pad) .. str .. string.rep(" ", width - len - pad)
end

local function GetPvpAchievementSummary(entry)
    local summary = {}
    local highestRank = nil
    local highestRankIndex = 0
    local highestPrefix = nil
    local highestLabel  = nil

    for _, col in ipairs(PvpRankColumns) do
        summary[col.key] = 0
    end

    for _, val in pairs(entry) do
        if type(val) == "string" then
            local name = val:lower()

            for i, col in ipairs(PvpRankColumns) do
                if col.prefix and name:find(col.prefix:lower(), 1, true) then
                    summary[col.key] = summary[col.key] + 1
                    if i > highestRankIndex then
                        highestRank = val  -- ✅ store actual match string
                        highestRankIndex = i
                        highestPrefix = col.prefix
                        highestLabel  = col.label
                    end
                elseif col.r1 then
                    for _, r1 in ipairs(R1Titles) do
                        if name == r1:lower() then
                            summary[col.key] = summary[col.key] + 1
                            if i > highestRankIndex then
                                highestRank = r1
                                highestRankIndex = i
                                highestPrefix = "Rank 1"
                                highestLabel  = col.label
                            end
                        end
                    end
                elseif col.hero then
                    for _, hero in ipairs(HeroTitles) do
                        if name:find(hero:lower(), 1, true) then
                            summary[col.key] = summary[col.key] + 1
                            if i > highestRankIndex then
                                highestRank = hero
                                highestRankIndex = i
                                highestPrefix = "Hero"
                                highestLabel  = col.label
                            end
                        end
                    end
                end
            end
        end
    end

    return { summary = summary, highest = highestRank, prefix = highestPrefix, label = highestLabel }
end

local function centerIcon(iconTag, width)
    local len = 3 -- 3 visual units for the icon
    local pad = math.max(0, math.floor((width - len) / 2))
    return string.rep(" ", pad) .. iconTag .. string.rep(" ", width - len - pad)
end

-- Build a |T...|t tag, optionally tinted via vertex color.
-- UI_RankedPvP_0X_Small icons are 64x64, so we can use full coords safely. :contentReference[oaicite:3]{index=3}
local function MakeIconTag(texturePath, size, offsetY, tint)
    offsetY = offsetY or 0
    if tint and type(tint) == "table" then
        local r = math.floor(((tint[1] or 1) * 255) + 0.5)
        local g = math.floor(((tint[2] or 1) * 255) + 0.5)
        local b = math.floor(((tint[3] or 1) * 255) + 0.5)

        -- |T texture:height:width:offsetX:offsetY:textureW:textureH:left:right:top:bottom:r:g:b |t :contentReference[oaicite:4]{index=4}
        return string.format("|T%s:%d:%d:0:%d:64:64:0:64:0:64:%d:%d:%d|t",
            texturePath, size, size, offsetY, r, g, b)
    end

    return string.format("|T%s:%d:%d:0:%d|t", texturePath, size, size, offsetY)
end

local function AddAchievementInfoToTooltip(tooltip, overrideName, overrideRealm)
    -- Only hook OnHide once per tooltip to avoid stacking thousands of handlers
    if not tooltip.__RatedStatsOnHideHooked then
        tooltip.__RatedStatsOnHideHooked = true
        tooltip:HookScript("OnHide", function(tip)
            tip.__RatedStatsLast = nil
        end)
    end

    local _, unit = tooltip:GetUnit()
    local name, realm

    if unit and UnitIsPlayer(unit) then
        name, realm = UnitFullName(unit)
    else
        name, realm = overrideName, overrideRealm
    end

    if not name then return end
    realm = realm or GetRealmName()
    local key = (name .. "-" .. realm):lower()

    -- Avoid adding twice for same target, but allow refreshes
    if tooltip.__RatedStatsLast == key then return end
    tooltip.__RatedStatsLast = key

    -- look up our per-char database and bail out if Achiev is off
    local key = UnitName("player") .. "-" .. GetRealmName()
    local db  = RSTATS.Database[key]
	local module = "RatedStats_Achiev"
    if C_AddOns.GetAddOnEnableState(module, nil) == 0 then
        return
    end
  
    local baseName, realm

    -- Use override only if tooltip:GetUnit() is not supported or not a unit tooltip
    local unit
    if tooltip.GetUnit then
        _, unit = tooltip:GetUnit()
    end

    if unit and UnitIsPlayer(unit) then
        baseName, realm = UnitFullName(unit)
    elseif overrideName then
        baseName = overrideName
        realm = overrideRealm or GetRealmName()
    else
        return -- No usable name/realm source
    end

    realm = realm or GetRealmName()
    local normRealm = NormalizeRealmSlug(realm)
    local fullName  = (baseName .. "-" .. normRealm):lower()

    -- Cache lookup using regionLookup (main OR alt)
    if achievementCache[fullName] == nil then
        local entry = regionLookup[fullName]
        if entry then
            achievementCache[fullName] = GetPvpAchievementSummary(entry)
        else
            achievementCache[fullName] = { summary = {}, highest = nil }
        end
    end

    local result = achievementCache[fullName]
    -- Safety: never allow non-table cache entries to break the tooltip
    if type(result) ~= "table" then
        result = { summary = {}, highest = nil }
        achievementCache[fullName] = result
    end	local summary = result.summary or {}
	local highest = result.highest

    tooltip:AddLine("|cffb69e86Rated Stats - Achievements|r")
    tooltip:AddLine("----------------------------")

    local hasAnyHistory = false
    for _, col in ipairs(PvpRankColumns) do
        if not col.hidden then
            if summary[col.key] and summary[col.key] > 0 then
                hasAnyHistory = true
                break
            end
        end
    end
	
    if highest then
        tooltip:AddLine("|cff00ff00Highest PvP Rank:|r " .. highest)
    else
        tooltip:AddLine("|cffff0000No History / Not Seen in Bracket|r")
    end

	if hasAnyHistory then
		local iconRow, valueRow = "", ""
		local iconSize = 16
		local iconOffsetY = 0
	
        for _, col in ipairs(PvpRankColumns) do
            if not col.hidden then
                local count = summary[col.key] or 0

                -- Hero: two icons
                if col.hero and col.icons then
                    local icons = ""
                    for _, iconPath in ipairs(col.icons) do
                        icons = icons .. string.format("|T%s:%d:%d:0:%d|t", iconPath, iconSize, iconSize, iconOffsetY)
                    end
                    iconRow  = iconRow  .. centerIcon(icons, 10)
                    valueRow = valueRow .. centerText(count, 12)

                else
                    local iconTag = MakeIconTag(
                        col.icon or "Interface\\Icons\\inv_misc_questionmark",
                        iconSize,
                        iconOffsetY,
                        col.tint
                    )
                    iconRow  = iconRow  .. centerIcon(iconTag, 6)
                    valueRow = valueRow .. centerText(count, 6)
                end
			end
		end

        tooltip:AddLine(iconRow)
        tooltip:AddLine(valueRow)
	end

    tooltip:Show()
end

-- Minimal ScrollBoxUtil helper (mirrors Raider.IO core.lua)
local ScrollBoxUtil = {}

function ScrollBoxUtil:OnViewFramesChanged(scrollBox, callback)
    if not scrollBox then return end
    if scrollBox.GetFrames then
        local frames = scrollBox:GetFrames()
        if frames then
            callback(frames, scrollBox)
        end
        scrollBox:RegisterCallback(ScrollBoxListMixin.Event.OnUpdate, function()
            local updated = scrollBox:GetFrames()
            if updated then
                callback(updated, scrollBox)
            end
        end)
    end
end

local lastTooltipUnit = nil

-- Defer hook until player is fully in the game
local f = CreateFrame("Frame")
f:RegisterEvent("PLAYER_LOGIN")
f:RegisterEvent("UPDATE_MOUSEOVER_UNIT")
f:RegisterEvent("PLAYER_TARGET_CHANGED")
f:RegisterEvent("PLAYER_FOCUS_CHANGED")

f:SetScript("OnEvent", function(_, event)
    if event == "PLAYER_LOGIN" then

        -- Table to cache the most recent applicant names for use in tooltips
        local recentApplicants = {}

        -- Capture real applicant names when players apply to *your* listing
        local appWatcher = CreateFrame("Frame")
        appWatcher:RegisterEvent("LFG_LIST_APPLICATION_STATUS_UPDATED")
        appWatcher:SetScript("OnEvent", function(_, _, applicantID, status)
            if status ~= "applied" then return end
            local activeEntry = C_LFGList.GetActiveEntryInfo()
            if not activeEntry or not activeEntry.activityID then return end

            local appInfo = C_LFGList.GetApplicantInfo(applicantID)
            if not appInfo then return end
            for i = 1, appInfo.numMembers do
                local fullName = select(1, C_LFGList.GetApplicantMemberInfo(applicantID, i))
                if fullName and fullName ~= "" then
                    recentApplicants[applicantID .. "-" .. i] = fullName
                end
            end
        end)

		-- Hook 1: General player units (includes mouseover, target, focus)
		hooksecurefunc(GameTooltip, "SetUnit", function(tooltip)
			local _, unit = tooltip:GetUnit()
			if not unit or not UnitIsPlayer(unit) then return end
		
			local name, realm = UnitFullName(unit)
			realm = realm or GetRealmName()
		
			-- Target/focus sometimes need a slight delay for text lines to exist
			local delay = (unit == "target" or unit == "focus") and 0.5 or 0.5
		
			C_Timer.After(delay, function()
				if tooltip:IsShown() and UnitIsPlayer(unit) then
					AddAchievementInfoToTooltip(tooltip, name, realm)
				end
			end)
		end)
		
		-- Hook 2: Ensure the player's own tooltip *always* updates cleanly
		hooksecurefunc(GameTooltip, "SetUnit", function(tooltip)
			local _, unit = tooltip:GetUnit()
			if unit == "player" then
				local name, realm = UnitFullName("player")
				realm = realm or GetRealmName()
--				tooltip.__RatedStatsLast = nil -- force refresh
				if tooltip:IsShown() then
					AddAchievementInfoToTooltip(tooltip, name, realm)
				end
			end
		end)

		-- Hook UnitFrame mouseovers (party/raid frames etc.)
		hooksecurefunc("UnitFrame_OnEnter", function(self)
			if not self or not self.unit or not UnitIsPlayer(self.unit) then return end
            if GameTooltip:IsForbidden() then return end  -- prevents blink + hide cycle

           -- Blizzard suppresses CompactUnitFrame tooltips depending on UI settings.
           -- Force the tooltip to exist so our addon can append lines.
           if not GameTooltip:IsShown() then
               GameTooltip:SetOwner(self, "ANCHOR_RIGHT")
               GameTooltip:SetUnit(self.unit)
           end

			local name, realm = UnitFullName(self.unit)
			realm = realm or GetRealmName()
		
			-- Delay a touch to ensure tooltip lines are added
--			C_Timer.After(0.5, function()
				if GameTooltip:IsShown() then
					AddAchievementInfoToTooltip(GameTooltip, name, realm)
				end
--			end)
		end)

		-- Hook CompactUnitFrame mouseovers (used by modern party/raid/enemy frames)
		if TooltipDataProcessor then
			TooltipDataProcessor.AddTooltipPostCall(Enum.TooltipDataType.Unit, function(tooltip, data)
				if not tooltip or not data or not data.unit then return end
				if not UnitIsPlayer(data.unit) then return end
				local name, realm = UnitFullName(data.unit)
				realm = realm or GetRealmName()
				if name and realm then
					AddAchievementInfoToTooltip(tooltip, name, realm)
				end
			end)
		else
			-- Fallback: use general GameTooltip::SetUnit hook if TooltipDataProcessor unavailable
			hooksecurefunc(GameTooltip, "SetUnit", function(tooltip)
				local _, unit = tooltip:GetUnit()
				if unit and UnitIsPlayer(unit) then
					local name, realm = UnitFullName(unit)
					realm = realm or GetRealmName()
					C_Timer.After(0.5, function()
						if tooltip:IsShown() then
							AddAchievementInfoToTooltip(tooltip, name, realm)
						end
					end)
				end
			end)
		end

        -- Hook LFG tooltips
        hooksecurefunc("LFGListUtil_SetSearchEntryTooltip", function(tooltip, resultID)
            local _, _, name, _, _, _, _, _, _, _, _, leaderName = C_LFGList.GetSearchResultInfo(resultID)
            if leaderName then
                local realm = GetNormalizedRealmName() or GetRealmName()

                -- Delay to ensure other tooltip extensions (e.g., RaiderIO) have run
                C_Timer.After(0.5, function()
                    if tooltip and tooltip:IsShown() then
                        -- Ensure correct anchor if needed
                        if not tooltip:GetOwner() then
                            tooltip:SetOwner(UIParent, "ANCHOR_CURSOR")
                        end
                        tooltip:AddLine(" ")
                        -- Append your achievement info
                        AddAchievementInfoToTooltip(tooltip, leaderName, realm)
                    end
                end)
            end
        end)

        -- Hook applicant tooltip popout (like Raider.IO)
        local function TryHookApplicantTooltip()
            local mixin = _G.TooltipLFGApplicantMixin
            if type(mixin) == "table" and mixin.SetApplicantMember then
                hooksecurefunc(mixin, "SetApplicantMember", function(self, applicantID, memberIdx)
                    C_Timer.After(0.5, function()
                        -- Prefer the full name captured from the application event
                        local fullName = recentApplicants[applicantID .. "-" .. memberIdx]
                        if not fullName or fullName == "" then
                            fullName = select(1, C_LFGList.GetApplicantMemberInfo(applicantID, memberIdx))
                        end

                        if fullName and fullName ~= "" then
                            local baseName, realm = strsplit("-", fullName)
                            realm = realm or GetRealmName()
                            AddAchievementInfoToTooltip(self, baseName, realm)
                        end
                    end)
                end)
                return true
            end
            return false
        end

        -- Keep retrying until the mixin exists
        local function WaitForMixin()
            if not TryHookApplicantTooltip() then
                C_Timer.After(0.5, WaitForMixin)
            end
        end
        C_Timer.After(0.5, WaitForMixin)

        -- Hook CommunitiesFrame (Guild Roster) ScrollBox row tooltips
        local function HookCommunitiesGuildRows()
            local container = CommunitiesFrame and CommunitiesFrame.MemberList and CommunitiesFrame.MemberList.ScrollBox
            if not container then return end

            local function HookRow(frame)
                if frame.__ratedStatsHooked then return end
                frame.__ratedStatsHooked = true

                frame:HookScript("OnEnter", function(self)
                    local info = self.memberInfo
                    if not info or not info.name then return end

                    local name, realm = strsplit("-", info.name)
                    realm = realm or GetRealmName()
                    AddAchievementInfoToTooltip(GameTooltip, name, realm)
                end)
            end

            container:RegisterCallback("OnAcquiredFrame", function(_, frame)
                if type(frame) == "table" and frame.GetObjectType then
                    HookRow(frame)
                end
            end, true)
        end

        C_Timer.After(0.5, HookCommunitiesGuildRows)

        -- Hook applicant rows in LFG
        local function HookApplicantFrames()
            local scrollBox = LFGListFrame and LFGListFrame.ApplicationViewer and LFGListFrame.ApplicationViewer.ScrollBox
            if not scrollBox or not scrollBox.GetFrames then
                C_Timer.After(0.5, HookApplicantFrames)
                return
            end

            local hooked = {}
            local function OnEnter(self)
                if self.applicantID and self.Members then
                    for _, member in pairs(self.Members) do
                        if not hooked[member] then
                            hooked[member] = true
                            member:HookScript("OnEnter", function(memberFrame)
                                local applicantID = memberFrame:GetParent().applicantID
                                local idx = memberFrame.memberIdx or 1
                                local fullName = recentApplicants[applicantID .. "-" .. idx]

                                -- Fallback if cache missed
                                if (not fullName or fullName == "") and applicantID then
                                    fullName = select(1, C_LFGList.GetApplicantMemberInfo(applicantID, idx))
                                end

                                if fullName and fullName ~= "" then
                                    local baseName, realm = strsplit("-", fullName)
                                    realm = realm or GetRealmName()
                                    AddAchievementInfoToTooltip(GameTooltip, baseName, realm)
                                end
                            end)
                            member:HookScript("OnLeave", function() GameTooltip:Hide() end)
                        end
                    end
                elseif self.memberIdx then
                    local parent = self:GetParent()
                    local idx = self.memberIdx
                    local fullName = recentApplicants[applicantID .. "-" .. idx]
                    if (not fullName or fullName == "") and applicantID then
                        fullName = select(1, C_LFGList.GetApplicantMemberInfo(applicantID, idx))
                    end
                    if fullName and fullName ~= "" then
                        local baseName, realm = strsplit("-", fullName)
                        realm = realm or GetRealmName()
                        AddAchievementInfoToTooltip(GameTooltip, baseName, realm)
                    end
                end
            end

            local frames = scrollBox:GetFrames()
            if not frames or #frames == 0 then
                C_Timer.After(0.5, HookApplicantFrames)
                return
            end

			local function HookRow(frame)
				if not frame or hooked[frame] then return end
				hooked[frame] = true
				frame:HookScript("OnEnter", OnEnter)
				frame:HookScript("OnLeave", function() GameTooltip:Hide() end)
			end
			
			for _, frame in ipairs(frames) do
				HookRow(frame)
			end
			
			scrollBox:RegisterCallback("OnAcquiredFrame", function(_, frame)
				HookRow(frame)
			end, true)
        end

        C_Timer.After(0.5, HookApplicantFrames)

	elseif event == "UPDATE_MOUSEOVER_UNIT" then
		if not UnitExists("mouseover") or not UnitIsPlayer("mouseover") then
			return
		end
	
		-- Safely get mouse focus for all modern WoW versions
		local mf = GetMouseFocus and GetMouseFocus()
			or (UIParent and UIParent.GetMouseFocus and UIParent:GetMouseFocus())
			or TheMouseFocus
	
		-- Prevent double/triple injections on unitframes (party/raid/arena/nameplate)
		if mf and (mf.unit or mf.displayedUnit or (mf.GetUnit and mf:GetUnit())) then
			return
		end
	
		if GameTooltip:IsShown() then
			local name, realm = UnitFullName("mouseover")
			realm = realm or GetRealmName()
			AddAchievementInfoToTooltip(GameTooltip, name, realm)
		end
--
--    elseif event == "PLAYER_TARGET_CHANGED" then
--        if UnitExists("target") and UnitIsPlayer("target") then
--            GameTooltip:SetUnit("target")
--        end
--
--    elseif event == "PLAYER_FOCUS_CHANGED" then
--        if UnitExists("focus") and UnitIsPlayer("focus") then
--            GameTooltip:SetUnit("focus")
--        end
    end
end) -- closes f:SetScript

-- === RatedStats: LFG Search Popout (Leader) → Append Achievements ===
-- Additive, self-contained; waits for LFG code to load, then appends our block.
do
    local function AppendLeaderAchievements(tooltip, resultID)
        if not tooltip or not resultID or type(AddAchievementInfoToTooltip) ~= "function" then return end

        -- Retail 11.x returns a table; older builds returned varargs. Handle both.
        local info = C_LFGList.GetSearchResultInfo(resultID)
        local leaderName
        if type(info) == "table" then
            leaderName = info.leaderName
        else
            local _1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_leader = C_LFGList.GetSearchResultInfo(resultID)
            leaderName = _leader
            info = nil
        end
        if not leaderName or leaderName == "" then return end

        -- Parse "Name-Realm" if present; never assume our realm.
        local baseName, realm = leaderName, nil
        if string.find(leaderName, "-", 1, true) then
            baseName, realm = strsplit("-", leaderName)
        end
        realm = (realm or (info and info.leaderRealm) or GetRealmName()):gsub("%s+", "")

        -- Let Blizzard/other addons build their lines first, then append ours.
        C_Timer.After(0.5, function()
            if tooltip:IsShown() then
                AddAchievementInfoToTooltip(tooltip, baseName, realm)
            end
        end)
    end

    -- Defer until LFG function exists (no edits to your existing init flow).
    local function TryHook()
        if type(_G.LFGListUtil_SetSearchEntryTooltip) == "function" then
            hooksecurefunc("LFGListUtil_SetSearchEntryTooltip", AppendLeaderAchievements)
            return true
        end
    end

    local waiter = CreateFrame("Frame")
    waiter:RegisterEvent("PLAYER_LOGIN")
    waiter:SetScript("OnEvent", function(self)
        local function poll()
            if not TryHook() then
                C_Timer.After(0.5, poll)
            else
                self:UnregisterAllEvents()
            end
        end
        poll()
    end)
end

-----------------------------------------------------------
-- RatedStats: PvP Queue and Instance Announcements
-----------------------------------------------------------

local function GetMySettings()
    local key = UnitName("player") .. "-" .. GetRealmName()
    local db = RSTATS and RSTATS.Database and RSTATS.Database[key]
    return db and db.settings or nil
end

-- Dropdown values:
-- 1=self (print), 2=party, 3=instance, 4=say, 5=yell
local function GetAnnounceTargetForCurrentMatch()
    local settings = GetMySettings()
    if not settings then
        return 1
    end

    if C_PvP and (
        (C_PvP.IsRatedSoloShuffle and C_PvP.IsRatedSoloShuffle()) or
        (C_PvP.IsSoloShuffle and C_PvP.IsSoloShuffle()) or
        (C_PvP.IsBrawlSoloShuffle and C_PvP.IsBrawlSoloShuffle())
    ) then
        return settings.achievAnnounceSS or 3
    end

    local inInstance, instanceType = IsInInstance()
    if inInstance and instanceType == "arena" then
        local enemyCount = (GetNumArenaOpponentSpecs and GetNumArenaOpponentSpecs()) or 0
        if enemyCount == 2 then
            return settings.achievAnnounce2v2 or 2
        elseif enemyCount == 3 then
            return settings.achievAnnounce3v3 or 2
        end
        return settings.achievAnnounce3v3 or settings.achievAnnounce2v2 or 2
    end

    if C_PvP and (C_PvP.IsRatedBattleground and C_PvP.IsRatedBattleground()) then
        return settings.achievAnnounceRBG or 1
    end

    if C_PvP and (C_PvP.IsSoloRBG and C_PvP.IsSoloRBG()) then
        return settings.achievAnnounceRBGB or 1
    end

    -- Random BG / unknown: keep it self to avoid spam.
    return 1
end

local function ResolveChatChannelFromTarget(target)
    -- 0=none, 1=self, 2=party, 3=instance, 4=say, 5=yell, 6=raid, 7=party(only5)
    if target == 0 or target == nil then
        return nil
    end

    if target == 2 then
        -- PARTY means PARTY (not instance chat), unless we're literally a raid.
        if IsInRaid() then return "RAID" end
        if IsInGroup() then return "PARTY" end
        return nil
    end

    if target == 3 then
        -- INSTANCE means instance group chat when available.
        if IsInGroup(LE_PARTY_CATEGORY_INSTANCE) then return "INSTANCE_CHAT" end
        -- If not available, fall back to group chat if any.
        if IsInRaid() then return "RAID" end
        if IsInGroup() then return "PARTY" end
        return nil
    end

    if target == 4 then return "SAY" end
    if target == 5 then return "YELL" end

    if target == 6 then
        if IsInRaid() then return "RAID" end
        -- If someone selects RAID while not in a raid, fall back to PARTY if grouped.
        if IsInGroup() then return "PARTY" end
        return nil
    end

    if target == 7 then
        -- Party (only 5): only send if we're actually a party-sized group.
        if IsInRaid() then return nil end
        if IsInGroup() then
            local n = GetNumGroupMembers()
            if n and n <= 5 then
                return "PARTY"
            end
        end
        return nil
    end

    return nil
end

local function AnnounceLine(message, target)
    if not message or message == "" then return end

    if target == 0 then
        return
    end

    if target == 1 then
        print(message)
        return
    end

    local channel = ResolveChatChannelFromTarget(target)
    if not channel then
        print(message)
        return
    end

    -- Chat has strict length limits; your padded formats can exceed them.
    if #message > 250 then
        message = message:gsub("%s%s+", " ")
    end
    if #message > 250 then
        print(message)
        return
    end

    SendChatMessage(message, channel)
end

local function PrintPartyAchievements()
    local settings = GetMySettings()
    if settings and settings.achievAnnounceOnQueue == false then return end

    if not IsInGroup() then return end

    local channel

    -- Prefer instance group chat only if we actually have an instance-group channel.
    if IsInGroup(LE_PARTY_CATEGORY_INSTANCE) then
        channel = "INSTANCE_CHAT"
    elseif IsInRaid() then
        channel = "RAID"
    else
        channel = "PARTY"
    end

    if channel then
        SendChatMessage("Rated Stats - Achievements for Group", channel)
    else
        print("Rated Stats - Achievements for Group")
    end

    local function AnnounceMember(baseName, realm)
        if not baseName or baseName == "" then return end
        realm = realm or GetRealmName()
        local normRealm = NormalizeRealmSlug(realm)
        local fullName  = (baseName .. "-" .. normRealm):lower()

        local cached = achievementCache[fullName]
        if not cached then
            local entry = regionLookup[fullName]
            if entry then
                cached = GetPvpAchievementSummary(entry)
                achievementCache[fullName] = cached
            end
        end

        local label = cached and cached.label or "Not Seen in Bracket"
        local msg = " - " .. baseName .. ": " .. label
        if channel then
            SendChatMessage(msg, channel)
        else
            print(msg)
        end
    end

    if IsInRaid() then
        for i = 1, GetNumGroupMembers() do
            local name = GetRaidRosterInfo(i)
            if name then
                local baseName, realm = strsplit("-", name)
                AnnounceMember(baseName, realm)
            end
        end
    else
        -- In a party, iterate party units (party1..party4) + include player.
        local myName, myRealm = UnitFullName("player")
        AnnounceMember(myName, myRealm)

        for i = 1, GetNumSubgroupMembers() do
            local unit = "party" .. i
            if UnitExists(unit) then
                local baseName, realm = UnitFullName(unit)
                AnnounceMember(baseName, realm)
            end
        end
    end
end

-- Only announce for these queue messages
local RatedQueueTriggers = {
    ["Your group has joined the queue for Random Battleground."] = true,
    ["Your group has joined the queue for Arena Skirmish."] = true,
    ["Your group has joined the queue for 2v2."] = true,
    ["Your group has joined the queue for 3v3."] = true,
    ["Your group has joined the queue for Rated Battleground."] = true,
    ["Your group has joined the queue for Rated Battleground Blitz."] = true,
}

-- === Queue watcher: fires once per queue start ===
local queueState = { "none", "none", "none" }
local queueWatcher = CreateFrame("Frame")
-- queueWatcher:RegisterEvent("UPDATE_BATTLEFIELD_STATUS")
queueWatcher:RegisterEvent("PVPQUEUE_ANYWHERE_SHOW")
queueWatcher:RegisterEvent("CHAT_MSG_SYSTEM")

local lastQueued = 0
local lastAllowedQueueMsg = 0

queueWatcher:SetScript("OnEvent", function(_, event, ...)
    local now = GetTime()

    -- System message check: only fire on exact PvP queue names
    if event == "CHAT_MSG_SYSTEM" then
        local msg = ...
        if RatedQueueTriggers[msg] then
            lastQueued = now
            lastAllowedQueueMsg = now
            C_Timer.After(1.0, PrintPartyAchievements)
        end
        return
    end

    -- Skip if still cooling down from last queue trigger
    if (now - lastQueued) < 10 then return end

    -- Skip if we've entered or are still inside an active PvP match
    local inInstance, instanceType = IsInInstance()
    if (inInstance and (instanceType == "pvp" or instanceType == "arena")) then return end

    -- Skip if a match became active recently (still resolving rounds)
    if (now - lastMatchActive) < 60 then return end

    -- Check all PvP queues
    for i = 1, 3 do
        local status = select(1, GetBattlefieldStatus(i))
        -- Fire only when transitioning into queued
        if status == "queued" and queueState[i] ~= "queued" then
            -- Only allow battlefield-triggered prints if we *just* saw an allowed queue message
            if (now - lastAllowedQueueMsg) > 5 then
                queueState[i] = status
                break
            end
            queueState[i] = "queued"
            lastQueued = now
            C_Timer.After(1.0, PrintPartyAchievements)
            return
        end

        -- Update state (must always run)
        queueState[i] = status
    end
end)

-----------------------------------------------------------
-- RatedStats: Post Team Summary after entering PvP instance
-----------------------------------------------------------

local function PostPvPTeamSummary()
    if not IsInInstance() then return end
    local inInstance, instanceType = IsInInstance()
    if not (inInstance and (instanceType == "pvp" or instanceType == "arena")) then return end

    local myTeam = {}
    local enemyTeam = {}
    local seenEnemies = {}

    local function collectTeamData(unitPrefix, count, target)
        for i = 1, count do
            local unit = unitPrefix .. i
            if UnitExists(unit) and UnitIsPlayer(unit) and not UnitIsUnit(unit, "player") then
                local name, realm = UnitFullName(unit)
                realm = realm or GetRealmName()
                local normRealm = NormalizeRealmSlug(realm)
                local fullName  = (name .. "-" .. normRealm):lower()
                local cached = achievementCache[fullName]
                if not cached then
                    local entry = regionLookup[fullName]
                    if entry then
                        cached = GetPvpAchievementSummary(entry)
                        achievementCache[fullName] = cached
                    end
                end
                local label = cached and cached.label or "Not Seen in Bracket"
                table.insert(target, name .. " - " .. label)
            end
        end
    end

    -- Use 'party' units for small groups (arena / shuffle), 'raid' for Rated BGs
    if IsInRaid() then
        collectTeamData("raid", GetNumGroupMembers(), myTeam)
    else
        collectTeamData("party", GetNumGroupMembers() - 1, myTeam)
    end
    
    -- Only add yourself manually if you weren’t included by party/raid units
    local name, realm = UnitFullName("player")
    realm = realm or GetRealmName()
    local normRealm = NormalizeRealmSlug(realm)
    local fullName  = (name .. "-" .. normRealm):lower()
    local foundSelf = false
    for _, member in ipairs(myTeam) do
        if member:lower():find(fullName, 1, true) then
            foundSelf = true
            break
        end
    end
    if not foundSelf then
        local cached = achievementCache[fullName]
        if not cached then
            local entry = regionLookup[fullName]
            if entry then
                cached = GetPvpAchievementSummary(entry)
                achievementCache[fullName] = cached
            end
        end
        local label = cached and cached.label or "Not Seen in Bracket"
        local baseName = name
        table.insert(myTeam, 1, baseName .. " - " .. label)
    end

    -- Attempt enemy team collection (only works in rated battlegrounds/shuffle)
    local function addEnemy(unit)
        if not UnitExists(unit) or not UnitIsPlayer(unit) or UnitIsFriend("player", unit) then return end
        local name, realm = UnitFullName(unit)
        realm = realm or GetRealmName()
        local normRealm = NormalizeRealmSlug(realm)
        local fullName  = (name .. "-" .. normRealm):lower()
        local cached = achievementCache[fullName]
        if seenEnemies[fullName] then return end  -- skip duplicates
        seenEnemies[fullName] = true
        if not cached then
            local entry = regionLookup[fullName]
            if entry then
                cached = GetPvpAchievementSummary(entry)
                achievementCache[fullName] = cached
            end
        end
        local label = cached and cached.label or "Not Seen in Bracket"
        local baseName = name
        table.insert(enemyTeam, baseName .. " - " .. label)
    end

    -- Prefer nameplates, but fall back to arena enemies if available
    for i = 1, 20 do addEnemy("nameplate" .. i) end
    for i = 1, 6 do addEnemy("arena" .. i) end

    local target = GetAnnounceTargetForCurrentMatch()
    AnnounceLine("=== Rated Stats - Achievements PvP Summary ===", target)
    AnnounceLine(centerText("My Team", 45) .. " || " .. centerText("Enemy Team", 45), target)

    local maxRows = math.max(#myTeam, #enemyTeam)
    for i = 1, maxRows do
        local left = myTeam[i] or ""
        local right = enemyTeam[i] or ""
        AnnounceLine(centerText(left, 45) .. " || " .. centerText(right, 45), target)
    end
end

local instanceWatcher = CreateFrame("Frame")
instanceWatcher:RegisterEvent("PLAYER_ENTERING_WORLD")
instanceWatcher:RegisterEvent("PVP_MATCH_ACTIVE")

instanceWatcher:SetScript("OnEvent", function(_, event, ...)
    local inInstance, instanceType = IsInInstance()

    -- 🔸 PvP Instances (BG / RBG / Blitz)
    if event == "PLAYER_ENTERING_WORLD" then
        -- exclude arena/skirmish/shuffle; handled by PVP_MATCH_ACTIVE instead
        if inInstance and instanceType == "pvp" and not IsActiveBattlefieldArena() then            -- battlegrounds: enemy list available right away via GetBattlefieldScore()
            C_Timer.After(30, function()
                -- collect both teams based on battlefield score API
                local numScores = GetNumBattlefieldScores()
                if numScores and numScores > 0 then
                    local myFaction = UnitFactionGroup("player")
                    local myTeam, enemyTeam = {}, {}

					for i = 1, numScores do
						local name, _, _, _, _, factionIndex = GetBattlefieldScore(i)
						if name and factionIndex ~= nil then
							-- Convert numeric faction index (0 = Horde, 1 = Alliance)
							local faction = (factionIndex == 0) and "Horde" or "Alliance"
							local myFaction = UnitFactionGroup("player")
							local isEnemy = (faction ~= myFaction)
					
							local baseName, realm = strsplit("-", name)
							realm = realm or GetRealmName()
							local normRealm = NormalizeRealmSlug(realm)
                            local fullName  = (baseName .. "-" .. normRealm):lower()
					
							local cached = achievementCache[fullName]
							if not cached then
								local entry = regionLookup[fullName]
								if entry then
									cached = GetPvpAchievementSummary(entry)
									achievementCache[fullName] = cached
								end
							end
					
							local label = cached and cached.label or "Not Seen in Bracket"
							if isEnemy then
								table.insert(enemyTeam, string.format("%s - %s", baseName, label))
							else
								table.insert(myTeam, string.format("%s - %s", baseName, label))
							end
						end
					end

                    local target = GetAnnounceTargetForCurrentMatch()
                    AnnounceLine("|cffb69e86=== Rated Stats - Achievements ===|r", target)
                    local maxRows = math.max(#myTeam, #enemyTeam)
                    for i = 1, maxRows do
                        local left = myTeam[i] or ""
                        local right = enemyTeam[i] or ""
                        if myFaction == "Horde" then
                            -- apply colors
                            local myTeam  = "|cFFFF3333" .. left .. "|r"
                            local enemyTeam = "|cFF3366FF" .. right .. "|r"
                            AnnounceLine(string.format("%-45s || %-45s", myTeam, enemyTeam), target)
                        else
                            -- apply colors
                            local myTeam  = "|cFF3366FF" .. left .. "|r"
                            local enemyTeam = "|cFFFF3333" .. right .. "|r"
                            AnnounceLine(string.format("%-45s || %-45s", myTeam, enemyTeam), target)
                        end
                    end
                end
            end)
        end
    end

    -- 🔸 Arenas / Skirmishes / Solo Shuffle
    if event == "PVP_MATCH_ACTIVE" then
        if inInstance and instanceType == "arena" then
            lastMatchActive = GetTime()
            -- Fires once when gates open (Arenas, Skirmishes, Solo Shuffle)
            C_Timer.After(90, PostPvPTeamSummary)
        end
    end
end)

-- ---------------------------------------------------------------------------
-- Minimal public helpers for RatedStats UI (icon + tooltip reuse)
-- ---------------------------------------------------------------------------

-- Allow RatedStats to reuse the exact tooltip block we already generate.
_G.RSTATS_Achiev_AddAchievementInfoToTooltip = AddAchievementInfoToTooltip

-- Return: iconPath, highestText
-- iconPath is taken from the same PvpRankColumns you use in the tooltip.
_G.RSTATS_Achiev_GetHighestPvpRank = function(fullName)
    if type(fullName) ~= "string" or fullName == "" then return nil end

    local baseName, realm = strsplit("-", fullName)
    if not baseName or baseName == "" then return nil end
    realm = realm or GetRealmName()

    local normRealm = NormalizeRealmSlug(realm or "")
    local key = (baseName .. "-" .. normRealm):lower()

    -- prime cache the same way the tooltip does
    local result = achievementCache[key]
    if result == nil then
        local entry = regionLookup[key]
        if entry then
            result = GetPvpAchievementSummary(entry)
            achievementCache[key] = result
        else
            -- Keep cache shape consistent with the tooltip’s expectations
            result = { summary = {}, highest = nil }
            achievementCache[key] = result
            return nil
        end
    end

    if type(result) ~= "table" or not result.highest then
        return nil
    end

    if not result or not result.highest then return nil end

    local highest = result.highest
    local highestLower = highest:lower()

    -- Find the same icon family your tooltip rows are based on.
    local iconPath
    for _, col in ipairs(PvpRankColumns) do
        if col.prefix and highestLower:find(col.prefix:lower(), 1, true) then
            iconPath = col.icon or (col.icons and col.icons[1])
            break
        end
        if col.r1 and col.icons then
            for _, r1 in ipairs(R1Titles) do
                if highestLower == r1:lower() then
                    iconPath = col.icons[1]
                    break
                end
            end
            if iconPath then break end
        end
        if col.glad and col.icons then
            for _, g in ipairs(GladTitles) do
                if highestLower == g:lower() then
                    iconPath = col.icons[1]
                    break
                end
            end
            if iconPath then break end
        end
        if col.hero and col.icons then
            for _, h in ipairs(HeroTitles) do
                if highestLower:find(h:lower(), 1, true) then
                    iconPath = col.icons[1]
                    break
                end
            end
            if iconPath then break end
        end
    end

    return iconPath, highest
end